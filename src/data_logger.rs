use crate::{BenchResult, Error};
use std::fs::{remove_file, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DataLogger {}

impl DataLogger {
    pub fn log<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        results: BenchResult,
        file_name: &P,
    ) -> Result<(), Error> {
        let path = Path::new(file_name);
        let path = PathBuf::from(path);
        if path.exists() {
            remove_file(path.clone())?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path.clone())?;

        let mut writer = csv::Writer::from_writer(file);
        writer.write_record(results.header)?;
        for record in results.records {
            writer.write_record(record.fields)?;
        }

        writer.flush()?;

        Ok(())
    }
}
