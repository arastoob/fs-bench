
pub mod plotter;
pub mod data_logger;
pub mod micro;
pub mod error;


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