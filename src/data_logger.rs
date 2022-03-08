use crate::{BenchMode, BenchResult, Error};
use std::fs::{remove_file, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct DataLogger {
    pub fs_name: String,
    pub log_path: PathBuf,
}

impl DataLogger {
    pub fn new(fs_name: String, log_path: PathBuf) -> Result<Self, Error> {
        Ok(Self { fs_name, log_path })
    }

    pub fn log(&self, results: BenchResult, mode: &BenchMode) -> Result<PathBuf, Error> {
        // remove the log file if exist
        let file_name = format!(
            "{}_{}.csv",
            self.fs_name,
            mode.to_string()
        );
        let mut log_path = self.log_path.clone();
        log_path.push(file_name);
        if log_path.exists() {
            remove_file(log_path.clone())?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(log_path.clone())?;

        let mut writer = csv::Writer::from_writer(file);
        writer.write_record(results.header)?;
        for record in results.records {
            writer.write_record(record.fields)?;
        }

        writer.flush()?;

        Ok(log_path)
    }
}
