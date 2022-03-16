use crate::Error;
use rand::Rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::{Arc, Mutex};

/// A collection of data points with some statistical functions on the data
pub struct Sample {
    sample: Vec<f64>,
}

pub struct Quartiles {
    pub q1: f64, // first quartile, 25% of the data is below this point
    pub q3: f64, // third quartile, 75% of the data lies below this point
}

impl Sample {
    pub fn new<T>(sample: &Vec<T>) -> Self
    where
        T: Clone + std::convert::Into<f64>,
    {
        let sample: Vec<f64> = sample.iter().map(|val| val.clone().into()).collect();

        Self { sample }
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
    pub fn quartiles(&self) -> Quartiles {
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

        Quartiles { q1, q3 }
    }

    /// Return the interquartile range of sample points
    pub fn iqr(&self) -> f64 {
        let quartiles = self.quartiles();
        quartiles.q3 - quartiles.q1
    }

    /// Return the outliers of the sample points based on Tukey's Method
    ///
    /// In Tukey's Method, values less than (25th percentile - 1.5 * IQR) or
    /// greater than (75th percentile + 1.5 * IQR) are considered outliers.
    pub fn outliers(&self) -> Vec<f64> {
        let quartiles = self.quartiles();
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

        outliers
    }

    /// Bootstrap Sampling
    /// Bootstrap Sampling is a method that involves drawing of sample data repeatedly with
    /// replacement, from the sample points to estimate a population parameter (https://www.analyticsvidhya.com/blog/2020/02/what-is-bootstrap-sampling-in-statistics-and-machine-learning/)
    ///
    /// This method returns a vector containing the means of each resample
    fn bootstrap(&self, iterations: usize) -> Result<Vec<f64>, Error> {
        let len = self.sample.len();

        // The output of this method is a vector of size at least 20 so that we can use the z-scores
        // for calculating confidence interval, otherwise we have to use t-values.
        if len < 20 {
            return Err(Error::InvalidConfig(
                "The sample size is less than 20".to_string(),
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

            let resample = Sample::new(&resample);
            resample_means.lock().unwrap().push(resample.mean());
        });

        let resample_means = resample_means.lock().unwrap().clone();
        Ok(resample_means)
    }

    /// Calculate the confidence interval of means for the sample data.
    /// The confidence interval of mean in the sample points is a range of values we are fairly sure
    /// our true mean of the main population lies in (https://www.mathsisfun.com/data/confidence-interval.html).
    ///
    /// This method returns a range for sample points' mean. For a confidence level, say 95%,
    /// the true mean of the main population is in this range.
    pub fn confidence_interval(
        &self,
        confidence_level: f64,
        iterations: usize,
    ) -> Result<(f64, f64), Error> {
        if confidence_level < 0f64 || confidence_level > 1f64 {
            return Err(Error::InvalidConfig(
                "The confidence level should be in range (0, 1)".to_string(),
            ));
        }

        let mut means = self.bootstrap(iterations)?;
        means.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let confidence_level = confidence_level * 100f64;
        let first_percentile = (100f64 - confidence_level) / 2f64;
        let last_percentile = confidence_level + first_percentile;

        let lb_idx = ((first_percentile * means.len() as f64) / 100f64).ceil() as usize;
        let ub_idx = ((last_percentile * means.len() as f64) / 100f64).floor() as usize;

        Ok((means[lb_idx], means[ub_idx]))
    }
}
