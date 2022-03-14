use crate::Error;

#[derive(Debug)]
pub struct Statistics {
    data: Vec<f64>,
    len: f64,
    sum: f64
}

impl Statistics
{
    pub fn new<T>(data: &Vec<T>)  -> Self
        where T: Clone + std::convert::Into<f64>
    {
        let len = data.len() as f64;

        let data: Vec<f64> = data.iter().map(|val| val.clone().into()).collect();
        let sum: f64 = data.iter().sum();

        Self {
            data,
            len,
            sum
        }
    }

    pub fn mean(&self) -> Result<f64, Error> {
        if self.sum == f64::from(0) {
            return Ok(f64::from(0));
        }

        let mean = self.sum / self.len;

        Ok(mean)
    }

    // standard deviation
    pub fn std(&self) -> Result<f64, Error> {
        let mean = self.mean()?;

        if mean == f64::from(0) {
            return Ok(f64::from(0))
        }

        let deviations: Vec<f64> = self.data.iter()
            .map(|value| {
                let x  = value - mean;
                x * x
            }).collect();

        let deviations_sum: f64 = deviations.iter().sum();
        let variance = deviations_sum / self.len;
        let std = variance.sqrt(); // standard deviation

        Ok(std)
    }

    // coefficient of variation
    pub fn cv(&self) -> Result<f64, Error> {
        let mean = self.mean()?;
        let std = self.std()?;

        let cv = std / mean;

        if cv.is_nan() || cv.is_infinite() {
            Ok(f64::from(0))
        } else {
            Ok(cv)
        }
    }

    // find the first and third quartiles
    pub fn quartiles(&self) -> Result<(f64, f64), Error> {
        let mut data = self.data.clone();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = self.len as usize / 2;
        let (first, second) = if self.len % f64::from(2) != f64::from(0) {
            // len is odd
            (data[0..mid].to_vec(), data[mid + 1..].to_vec())
        } else {
            // len is even
            (data[0..mid].to_vec(), data[mid..].to_vec())
        };

        let q1 = first[first.len() / 2].clone();
        let q3 = second[second.len() / 2].clone();

        Ok((q1, q3))
    }

    // interquartile range
    pub fn iqr(&self) -> Result<f64, Error> {
        let (q1, q3) = self.quartiles()?;
        let iqr = q3 - q1;

        Ok(iqr)
    }

    // find the outliers of the data vector based on Tukey's Method:
    // In Tukey's Method, values less than (25th percentile - 1.5 * IQR) or
    // greater than (75th percentile + 1.5 * IQR) are considered outliers.
    pub fn outliers(&self) -> Result<Vec<f64>, Error> {
        let (q1, q3) = self.quartiles()?;
        let iqr = q3 - q1;

        let lower_limit = q1 - (iqr.clone() * 1.5);
        let upper_limit = (iqr * 1.5) + q3;

        let mut data = self.data.clone();
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
}