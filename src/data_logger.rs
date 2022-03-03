use std::fs::{OpenOptions, remove_file};
use std::path::Path;
use crate::{BenchResult, Error};

#[derive(Debug)]
pub struct DataLogger {
    pub fs_name: String,
    pub log_path: String,
}

impl DataLogger {
    pub fn new(fs_name: String, log_path: String) -> Result<Self, Error> {

        let (log_path, _) = log_path.rsplit_once("/").unwrap(); // remove / at the end
        Ok(Self {
            fs_name,
            log_path: log_path.to_string(),
        })
    }

    pub fn log(&self, results: BenchResult, bench_name: &str) -> Result<String, Error> {
        // remove the log file if exist
        let log_file_name = format!("{}/{}_{}.csv", self.log_path, self.fs_name, bench_name);
        let log_path = Path::new(&log_file_name);
        if log_path.exists() {
            remove_file(log_path).expect("removing the existing log file failed");
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(log_path)?;

        let mut writer = csv::Writer::from_writer(file);
        writer.write_record(results.header)?;
        for record in results.records {
            writer.write_record(record.fields)?;
        }

        writer.flush()?;

        Ok(log_file_name)
    }
}