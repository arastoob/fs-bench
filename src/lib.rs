
pub mod plotter;
pub mod data_logger;
pub mod micro;
pub mod error;


use std::fmt::{Display, Formatter};
use crate::error::Error;
use std::fs::{create_dir, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;


#[derive(Debug)]
pub enum BenchMode {
    OpsPerSecond,
    Throughput,
    Behaviour
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ops_per_second" => Ok(BenchMode::OpsPerSecond),
            "throughput" => Ok(BenchMode::Throughput),
            "behaviour" => Ok(BenchMode::Behaviour),
            _ => Err("valid benckmark modes are: ops_per_second, throughput, behaviour".to_string())
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
    pub records: Vec<Record>
}

impl BenchResult {
    pub fn new(header: Vec<String>) -> Self {
        BenchResult {
            header,
            records: vec![]
        }
    }

    pub fn add_record(&mut self, record: Record) -> Result<(), Error> {
        if record.fields.len() != self.header.len() {
            return Err(Error::Unknown("the record and header should be of the same length".to_string()));
        }
        self.records.push(record);

        Ok(())
    }
}

#[derive(Debug)]
pub struct Record {
    pub fields: Vec<String>
}

pub fn make_dir(dir: &str) -> Result<(), Error> {
    let path = Path::new(&dir);

    create_dir(path)?;

    Ok(())
}

pub fn make_file(file: &str) -> Result<File, Error> {
    let path = Path::new(&file);

    Ok(File::create(path)?)
}

pub fn write_file(file: &str, content: &mut Vec<u8>) -> Result<usize, Error> {
    let mut file = OpenOptions::new().write(true).append(false).open(file)?;

    match file.write(&content) {
        Ok(size) => Ok(size),
        Err(error) => Err(Error::IO(error)),
    }
}

pub fn read_file(file: &str, read_buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let mut file = OpenOptions::new().read(true).open(file)?;

    match file.read(read_buffer) {
        Ok(size) => Ok(size),
        Err(error) => Err(Error::IO(error)),
    }
}