use crate::error::Error;
use rand::Rng;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// A collection of data points with some statistical functions on the data
pub struct Statistics {
    sample: Vec<f64>,
}

pub struct Quartiles {
    pub q1: f64, // first quartile, 25% of the data is below this point
    pub q3: f64, // third quartile, 75% of the data lies below this point
}

impl Statistics {
    pub fn new<T>(sample: &[T]) -> Result<Self, Error>
    where
        T: Clone + std::convert::Into<f64>,
    {
        if sample.len() < 3 {
            return Err(Error::InvalidConfig(format!(
                "not enough samples (sample len is {})",
                sample.len()
            )));
        }
        let sample: Vec<f64> = sample.iter().map(|val| val.clone().into()).collect();

        Ok(Self { sample })
    }

    /// Return the mean of sample points
    pub fn mean(&self) -> f64 {
        let sum: f64 = self.sample.iter().sum();
        if sum == 0f64 {
            0f64
        } else {
            sum / self.sample.len() as f64
        }
    }

    /// Return the variance of sample points
    pub fn variance(&self) -> f64 {
        let mean = self.mean();

        if mean == 0f64 {
            0f64
        } else {
            let deviations_sum = self
                .sample
                .iter()
                .map(|value| (value - mean).powi(2))
                .fold(0f64, |acc, val| acc + val);

            deviations_sum / self.sample.len() as f64
        }
    }

    /// Return the standard deviation of sample points
    pub fn std(&self) -> f64 {
        self.variance().sqrt()
    }

    ///  Return the coefficient of variation of sample points
    pub fn cv(&self) -> f64 {
        let mean = self.mean();
        let std = self.std();

        let cv = std / mean;

        if cv.is_nan() || cv.is_infinite() {
            0f64
        } else {
            cv
        }
    }

    /// Find the first and third quartiles
    fn quartiles(&self) -> Result<Quartiles, Error> {
        if self.sample.len() < 3 {
            return Err(Error::InvalidConfig(
                "the sample size should be at least 3".to_string(),
            ));
        }
        let mut data = self.sample.clone();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let len = data.len();
        let mid = len / 2;
        let (first, second) = if len as f64 % 2f64 != 0f64 {
            // len is odd
            (data[0..mid].to_vec(), data[mid + 1..].to_vec())
        } else {
            // len is even
            (data[0..mid].to_vec(), data[mid..].to_vec())
        };

        let q1 = first[first.len() / 2].clone();
        let q3 = second[second.len() / 2].clone();

        Ok(Quartiles { q1, q3 })
    }

    /// Return the interquartile range of sample points
    pub fn iqr(&self) -> Result<f64, Error> {
        let quartiles = self.quartiles()?;
        Ok(quartiles.q3 - quartiles.q1)
    }

    /// Return the outliers of the sample points based on Tukey's Method
    ///
    /// In Tukey's Method, values less than (25th percentile - 1.5 * IQR) or
    /// greater than (75th percentile + 1.5 * IQR) are considered outliers.
    pub fn outliers(&self) -> Result<Vec<f64>, Error> {
        let quartiles = self.quartiles()?;
        let iqr = quartiles.q3 - quartiles.q1;

        let lower_limit = quartiles.q1 - (iqr.clone() * 1.5);
        let upper_limit = (iqr * 1.5) + quartiles.q3;

        let mut data = self.sample.clone();
        data.push(lower_limit.clone());
        data.push(upper_limit.clone());
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let lower_idx = data.iter().position(|val| *val == lower_limit).unwrap();
        let upper_idx = data.iter().position(|val| *val == upper_limit).unwrap();

        // the data lower than the lower_idx and higher than the upper_idx are outliers
        let mut outliers = vec![];
        outliers.append(&mut data[0..lower_idx].to_vec());
        outliers.append(&mut data[upper_idx + 1..].to_vec());

        Ok(outliers)
    }

    /// Calculate the confidence interval of mean for the sample data using bootstrap sampling.
    /// The confidence interval of mean in the sample points is a range of values we are fairly sure
    /// our true mean of the main population lies in (https://www.mathsisfun.com/data/confidence-interval.html).
    /// This method returns a range for sample points' mean, and the bootstrap sample means.
    /// For a confidence level, say 95%, the true mean of the main population is in this range.
    pub fn mean_confidence_interval(
        &self,
        confidence_level: f64,
        iterations: usize,
    ) -> Result<(f64, f64, Vec<f64>), Error> {
        if confidence_level < 0f64 || confidence_level > 1f64 {
            return Err(Error::InvalidConfig(
                "The confidence level should be in range (0, 1)".to_string(),
            ));
        }

        let means = self.bootstrap(iterations)?;
        let mut means_sorted = means.clone();
        means_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let confidence_level = confidence_level * 100f64;
        let first_percentile = (100f64 - confidence_level) / 2f64;
        let last_percentile = confidence_level + first_percentile;

        let lb_idx = ((first_percentile * means_sorted.len() as f64) / 100f64).ceil() as usize;
        let ub_idx = ((last_percentile * means_sorted.len() as f64) / 100f64).floor() as usize;

        Ok((means_sorted[lb_idx], means_sorted[ub_idx], means))
    }

    /// Bootstrap Sampling
    /// Bootstrap Sampling is a method that involves drawing of sample data repeatedly with
    /// replacement, from the sample points to estimate a population parameter (https://www.analyticsvidhya.com/blog/2020/02/what-is-bootstrap-sampling-in-statistics-and-machine-learning/)
    ///
    /// This method returns a vector containing the means of each resample
    fn bootstrap(&self, iterations: usize) -> Result<Vec<f64>, Error> {
        let len = self.sample.len();

        // The output of this method is a vector of size at least 30 so that we can use the z-scores
        // for calculating confidence interval, otherwise we have to use t-values.
        if len < 30 {
            return Err(Error::InvalidConfig(
                "The sample size is less than 30".to_string(),
            ));
        }

        let resample_means = Arc::new(Mutex::new(vec![]));
        (0..iterations).into_par_iter().for_each(|_| {
            let mut resample = vec![];
            while resample.len() < len {
                // get random samples repeatedly with replacement
                let idx = rand::thread_rng().gen_range(0..len);
                resample.push(self.sample[idx]);
            }

            let resample = Statistics::new(&resample).unwrap();
            resample_means.lock().unwrap().push(resample.mean());
        });

        let resample_means = resample_means.lock().unwrap().clone();
        Ok(resample_means)
    }

    pub fn analyse(&self) -> Result<AnalysedData, Error> {
        let (mean_lb, mean_ub, sample_means) = self.mean_confidence_interval(0.95, 1000)?;

        let mean = Statistics::new(&sample_means)?.mean();

        Ok(AnalysedData {
            mean: mean.floor(),
            mean_lb: mean_lb.floor(),
            mean_ub: mean_ub.floor(),
            sample_means,
        })
    }

    ///
    /// count the number of operations in a time window
    /// the time window length is in milliseconds
    /// the input times contains the timestamps in unix_time format. The first 10 digits are
    /// date and time in seconds and the last 9 digits show the milliseconds
    ///
    /// The output is a list of tuples including operation per seconds in a specific time: (time, ops_per_second)
    ///
    pub fn ops_in_window(
        times: &Vec<SystemTime>,
        duration: Duration,
    ) -> Result<Vec<(f64, usize)>, Error> {
        let len = times.len();
        let first = times[0]; // first timestamp
        let mut last = times[len - 1]; // last timestamp
        if last.duration_since(first)? > duration {
            last = first.add(duration);
        }

        // decide about the window length in millis
        let duration = last.duration_since(first)?.as_secs_f64();
        let window = if duration < 0.5 {
            2
        } else if duration < 1f64 {
            5
        } else if duration < 3f64 {
            10
        } else if duration < 5f64 {
            20
        } else if duration < 10f64 {
            50
        } else if duration < 20f64 {
            70
        } else if duration < 50f64 {
            100
        } else if duration < 100f64 {
            150
        } else if duration < 150f64 {
            200
        } else if duration < 200f64 {
            500
        } else if duration < 300f64 {
            1000
        } else {
            5000
        };

        let mut ops_in_window = vec![];

        let mut next = first.add(Duration::from_millis(window));
        let mut idx = 0;
        let mut ops = 0;
        while next < last {
            while times[idx] < next {
                // count ops in this time window
                ops += 1;
                idx += 1;
            }
            let time = next.duration_since(first)?.as_secs_f64();
            // we have counted ops in a window length milliseconds, so the ops in
            // a second is (ops * 1000) / window
            let ops_per_second = (ops * 1000) / window as usize;
            ops_in_window.push((time, ops_per_second));

            // go the next time window
            next = next.add(Duration::from_millis(window));
            ops = 0;
        }

        // count the remaining
        if idx < len {
            ops = len - idx;
            let time = last.duration_since(first)?.as_secs_f64();
            let ops_per_second = (ops * 1000) / window as usize;
            ops_in_window.push((time, ops_per_second));
        }

        Ok(ops_in_window)
    }
}

pub struct AnalysedData {
    pub mean: f64,
    pub mean_lb: f64,
    pub mean_ub: f64,
    pub sample_means: Vec<f64>,
}
