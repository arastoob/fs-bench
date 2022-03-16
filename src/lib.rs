pub mod data_logger;
pub mod error;
pub mod micro;
pub mod plotter;
pub mod sample;
mod timer;

use crate::error::Error;
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::{Display, Formatter};
use std::fs::{create_dir, remove_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub enum BenchMode {
    OpsPerSecond,
    Throughput,
    Behaviour,
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ops_per_second" => Ok(BenchMode::OpsPerSecond),
            "throughput" => Ok(BenchMode::Throughput),
            "behaviour" => Ok(BenchMode::Behaviour),
            _ => {
                Err("valid benckmark modes are: ops_per_second, throughput, behaviour".to_string())
            }
        }
    }
}

impl Display for BenchMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchMode::OpsPerSecond => write!(f, "ops_per_second"),
            BenchMode::Behaviour => write!(f, "behaviour"),
            BenchMode::Throughput => write!(f, "throughput"),
        }
    }
}

#[derive(Debug)]
pub struct BenchResult {
    pub header: Vec<String>,
    pub records: Vec<Record>,
}

impl BenchResult {
    pub fn new(header: Vec<String>) -> Self {
        BenchResult {
            header,
            records: vec![],
        }
    }

    pub fn add_record(&mut self, record: Record) -> Result<(), Error> {
        if record.fields.len() != self.header.len() {
            return Err(Error::Unknown(
                "the record and header should be of the same length".to_string(),
            ));
        }
        self.records.push(record);

        Ok(())
    }

    pub fn add_records(&mut self, records: Vec<Record>) -> Result<(), Error> {
        if records[0].fields.len() != self.header.len() {
            return Err(Error::Unknown(
                "the records and header should be of the same length".to_string(),
            ));
        }
        for record in records {
            self.records.push(record);
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Record {
    pub fields: Vec<String>,
}

pub fn make_dir(path: &PathBuf) -> Result<(), Error> {
    create_dir(path)?;

    Ok(())
}

pub fn make_file(path: &PathBuf) -> Result<File, Error> {
    Ok(File::create(path)?)
}

pub fn write_file(path: &PathBuf, content: &mut Vec<u8>) -> Result<usize, Error> {
    let mut file = OpenOptions::new().write(true).append(false).open(path)?;

    match file.write(&content) {
        Ok(size) => Ok(size),
        Err(error) => Err(Error::IO(error)),
    }
}

pub fn read_file(path: &PathBuf, read_buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let mut file = OpenOptions::new().read(true).open(path)?;

    match file.read(read_buffer) {
        Ok(size) => Ok(size),
        Err(error) => Err(Error::IO(error)),
    }
}

pub fn cleanup(path: &PathBuf) -> Result<(), Error> {
    // let bench_name = bench_name.to_string();
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner().template("{spinner} {msg}"));
    spinner.set_message(format!("clean up {}", path.to_str().unwrap()));

    let (sender, receiver) = channel();
    let path = path.clone();
    thread::spawn(move || {
        let path = Path::new(&path);
        if path.exists() {
            remove_dir_all(path).unwrap();
        }
        // notify the receiver about finishing the clean up
        sender.send(true).unwrap();
    });

    // spin the spinner until the clean up is done
    loop {
        match receiver.try_recv() {
            Ok(_done) => {
                // wait another 2 seconds
                for _ in 0..40 {
                    thread::sleep(Duration::from_millis(50));
                    spinner.inc(1);
                }
                spinner.finish_and_clear();
                break;
            }
            _ => {
                thread::sleep(Duration::from_millis(50));
                spinner.inc(1);
            }
        }
    }

    Ok(())
}
