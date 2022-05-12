use std::sync::{Arc, Mutex};
use rand::Rng;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use crate::Error;

/// A collection of data points with some statistical functions on the data
pub struct Sample {
    sample: Vec<f64>,
}

pub struct Quartiles {
    pub q1: f64, // first quartile, 25% of the data is below this point
    pub q3: f64, // third quartile, 75% of the data lies below this point
}

impl Sample {
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
    pub fn quartiles(&self) -> Result<Quartiles, Error> {
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

    /// Calculate the confidence interval error margin of the mean for the sample data.
    /// The confidence interval of mean in the sample points is a range of values we are fairly sure
    /// our true mean of the main population lies in (https://www.mathsisfun.com/data/confidence-interval.html).
    /// So, the confidence interval of mean is: (mean - error_margin, mean + error_margin)
    pub fn confidence_interval_error_margin(&self, confidence_level: f64) -> Result<f64, Error> {
        if confidence_level < 0f64 || confidence_level > 1f64 {
            return Err(Error::InvalidConfig(
                "The confidence level should be in range (0, 1)".to_string(),
            ));
        }

        // the error margin for a confidence level is:
        //      if sample size is bigger than 30: z_value * (sd / sqrt(n))
        //      if sample size is less than 30: t_value * (sd / sqrt(n))

        let n = self.sample.len();
        let sd = self.std();
        let sqrt_n = (n as f64).sqrt();
        if n >= 30 {
            let z_val = self.z_value(confidence_level)?;
            Ok(z_val * (sd / sqrt_n))
        } else {
            let t_val = self.t_value(confidence_level)?;
            Ok(t_val * (sd / sqrt_n))
        }
    }

    /// Calculate the confidence interval of mean for the sample data using bootstrap sampling.
    /// This method returns a range for sample points' mean. For a confidence level, say 95%,
    /// the true mean of the main population is in this range.
    pub fn mean_confidence_interval(&self, confidence_level: f64, iterations: usize) -> Result<(f64, f64), Error> {
        if confidence_level < 0f64 || confidence_level > 1f64 {
            return Err(Error::InvalidConfig("The confidence level should be in range (0, 1)".to_string()));
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
            return Err(Error::InvalidConfig("The sample size is less than 20".to_string()))
        }

        let resample_means = Arc::new(Mutex::new(vec![]));
        (0..iterations).into_par_iter().for_each(|_| {
            let mut resample = vec![];
            while resample.len() < len { // get random samples repeatedly with replacement
                let idx = rand::thread_rng().gen_range(0..len);
                resample.push(self.sample[idx]);
            }

            let resample = Sample::new(&resample).unwrap();
            resample_means.lock().unwrap().push(resample.mean());
        });

        let resample_means = resample_means.lock().unwrap().clone();
        Ok(resample_means)
    }

    fn z_value(&self, confidence_level: f64) -> Result<f64, Error> {
        match confidence_level {
            cl if cl == 0.7 => Ok(1.04),
            cl if cl == 0.75 => Ok(1.15),
            cl if cl == 0.8 => Ok(1.28),
            cl if cl == 0.85 => Ok(1.44),
            cl if cl == 0.9 => Ok(1.645),
            cl if cl == 0.92 => Ok(1.75),
            cl if cl == 0.95 => Ok(1.96),
            cl if cl == 0.96 => Ok(2.05),
            cl if cl == 0.98 => Ok(2.33),
            cl if cl == 0.99 => Ok(2.58),
            _ => Err(Error::InvalidConfig(format!(
                "invalid confidence level: {}",
                confidence_level
            ))),
        }
    }

    // if the sample size is less than 30, we need the t-value instead of z-value for calculating the ci
    fn t_value(&self, confidence_level: f64) -> Result<f64, Error> {
        // we use the t-values for calculating the confidence interval is the sample size is less than 30.
        if self.sample.len() > 30 {
            return Err(Error::InvalidConfig(
                "the sample size is more than 30".to_string(),
            ));
        }

        // The rows in the below t_distributions are for different degrees of freedom
        // (df = n - 1, n is the sample size) values starting from 1 to 30.
        // The values in each row are for different confidence level values: 60% 70% 80% 85% 90% 95% 98% 99% 99.8% 99.9%.
        let t_distributions = vec![
            [
                1.376, 1.963, 3.133, 4.195, 6.320, 12.69, 31.81, 63.67, 318.309, 636.619,
            ],
            [
                1.060, 1.385, 1.883, 2.278, 2.912, 4.271, 6.816, 9.520, 19.65, 26.30,
            ],
            [
                0.978, 1.25, 1.637, 1.924, 2.352, 3.179, 4.525, 5.797, 9.937, 12.39,
            ],
            [
                0.941, 1.19, 1.533, 1.778, 2.132, 2.776, 3.744, 4.596, 7.115, 8.499,
            ],
            [
                0.919, 1.156, 1.476, 1.699, 2.015, 2.57, 3.365, 4.03, 5.876, 6.835,
            ],
            [
                0.906, 1.134, 1.44, 1.65, 1.943, 2.447, 3.143, 3.707, 5.201, 5.946,
            ],
            [
                0.896, 1.119, 1.415, 1.617, 1.895, 2.365, 2.999, 3.5, 4.783, 5.403,
            ],
            [
                0.889, 1.108, 1.397, 1.592, 1.86, 2.306, 2.897, 3.356, 4.5, 5.039,
            ],
            [
                0.883, 1.1, 1.383, 1.574, 1.833, 2.262, 2.822, 3.25, 4.297, 4.78,
            ],
            [
                0.879, 1.093, 1.372, 1.559, 1.813, 2.228, 2.764, 3.17, 4.144, 4.586,
            ],
            [
                0.875, 1.088, 1.363, 1.548, 1.796, 2.201, 2.719, 3.106, 4.025, 4.437,
            ],
            [
                0.873, 1.083, 1.356, 1.538, 1.782, 2.179, 2.682, 3.055, 3.93, 4.318,
            ],
            [
                0.87, 1.079, 1.35, 1.53, 1.771, 2.16, 2.651, 3.013, 3.852, 4.221,
            ],
            [
                0.868, 1.076, 1.345, 1.523, 1.761, 2.145, 2.625, 2.977, 3.788, 4.141,
            ],
            [
                0.866, 1.074, 1.341, 1.517, 1.753, 2.131, 2.603, 2.947, 3.733, 4.073,
            ],
            [
                0.865, 1.071, 1.337, 1.512, 1.746, 2.12, 2.584, 2.921, 3.687, 4.015,
            ],
            [
                0.863, 1.069, 1.333, 1.508, 1.74, 2.11, 2.567, 2.899, 3.646, 3.965,
            ],
            [
                0.862, 1.067, 1.33, 1.504, 1.734, 2.101, 2.553, 2.879, 3.611, 3.922,
            ],
            [
                0.861, 1.066, 1.328, 1.5, 1.729, 2.093, 2.54, 2.861, 3.58, 3.884,
            ],
            [
                0.86, 1.064, 1.325, 1.497, 1.725, 2.086, 2.529, 2.846, 3.552, 3.85,
            ],
            [
                0.859, 1.063, 1.323, 1.494, 1.721, 2.08, 2.518, 2.832, 3.528, 3.82,
            ],
            [
                0.858, 1.061, 1.321, 1.492, 1.717, 2.074, 2.509, 2.819, 3.505, 3.792,
            ],
            [
                0.857, 1.06, 1.319, 1.489, 1.714, 2.069, 2.5, 2.808, 3.485, 3.768,
            ],
            [
                0.857, 1.059, 1.318, 1.487, 1.711, 2.064, 2.493, 2.797, 3.467, 3.746,
            ],
            [
                0.856, 1.058, 1.316, 1.485, 1.708, 2.06, 2.486, 2.788, 3.451, 3.725,
            ],
            [
                0.856, 1.058, 1.315, 1.483, 1.706, 2.056, 2.479, 2.779, 3.435, 3.707,
            ],
            [
                0.855, 1.057, 1.314, 1.482, 1.703, 2.052, 2.473, 2.771, 3.421, 3.69,
            ],
            [
                0.855, 1.056, 1.313, 1.48, 1.701, 2.048, 2.468, 2.764, 3.409, 3.674,
            ],
            [
                0.854, 1.055, 1.311, 1.479, 1.699, 2.045, 2.463, 2.757, 3.397, 3.66,
            ],
            [
                0.854, 1.055, 1.31, 1.477, 1.697, 2.042, 2.458, 2.75, 3.386, 3.646,
            ],
        ];

        let df = self.sample.len() - 1;
        match confidence_level {
            cl if cl == 0.6 => Ok(t_distributions[df][0]),
            cl if cl == 0.7 => Ok(t_distributions[df][1]),
            cl if cl == 0.8 => Ok(t_distributions[df][2]),
            cl if cl == 0.85 => Ok(t_distributions[df][3]),
            cl if cl == 0.9 => Ok(t_distributions[df][4]),
            cl if cl == 0.95 => Ok(t_distributions[df][5]),
            cl if cl == 0.98 => Ok(t_distributions[df][6]),
            cl if cl == 0.99 => Ok(t_distributions[df][7]),
            cl if cl == 0.998 => Ok(t_distributions[df][8]),
            cl if cl == 0.999 => Ok(t_distributions[df][9]),
            _ => Err(Error::InvalidConfig(format!(
                "invalid confidence level: {}",
                confidence_level
            ))),
        }
    }
}
