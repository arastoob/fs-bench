pub mod data_logger;
pub mod error;
pub mod micro;
pub mod plotter;
pub mod sample;
mod timer;
pub mod wasm_workload;
pub mod strace_workload;

use crate::error::Error;
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::{Display, Formatter};
use std::fs::{create_dir, remove_dir_all, File, OpenOptions, create_dir_all, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub enum BenchMode {
    Micro,
    Wasm,
    Strace,
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "micro" => Ok(BenchMode::Micro),
            "wasm" => Ok(BenchMode::Wasm),
            "strace" => Ok(BenchMode::Strace),
            _ => {
                Err("valid benckmark modes are: micro, wasm, strace".to_string())
            }
        }
    }
}

impl Display for BenchMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchMode::Micro => write!(f, "micro"),
            BenchMode::Wasm => write!(f, "workload"),
            BenchMode::Strace => write!(f, "strace"),
        }
    }
}


#[derive(Debug)]
pub enum ResultMode {
    OpsPerSecond,
    Throughput,
    Behaviour,
    OpTimes,
}

impl FromStr for ResultMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ops_per_second" => Ok(ResultMode::OpsPerSecond),
            "throughput" => Ok(ResultMode::Throughput),
            "behaviour" => Ok(ResultMode::Behaviour),
            _ => {
                Err("valid result modes are: ops_per_second, throughput, behaviour".to_string())
            }
        }
    }
}

impl Display for ResultMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultMode::OpsPerSecond => write!(f, "ops_per_second"),
            ResultMode::Behaviour => write!(f, "behaviour"),
            ResultMode::Throughput => write!(f, "throughput"),
            ResultMode::OpTimes => write!(f, "op_times"),
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

pub struct Fs {}

impl Fs {
    pub fn make_dir<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        create_dir(path)
    }

    pub fn make_dir_all<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        create_dir_all(path)
    }

    pub fn make_file<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        File::create(path)
    }

    pub fn open_file<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        OpenOptions::new().write(true).read(true).append(false).open(path)
    }

    pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        OpenOptions::new().read(true).open(path)
    }

    pub fn open_write<P: AsRef<Path>>(path: P, content: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().write(true).read(true).append(false).open(path)?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn open_write_at<P: AsRef<Path>>(path: P, content: &mut Vec<u8>, offset: u64,) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().write(true).read(true).append(false).open(path)?;
        file.seek(SeekFrom::Start(offset))?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn write(file: &mut File, content: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn write_at(file: &mut File, content: &mut Vec<u8>, offset: u64,) -> Result<usize, std::io::Error> {
        file.seek(SeekFrom::Start(offset))?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn open_read<P: AsRef<Path>>(path: P, read_buffer: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.read(read_buffer)
    }

    pub fn open_read_at<P: AsRef<Path>>(
        path: P,
        read_buffer: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(read_buffer)
    }

    pub fn read(file: &mut File, read_buffer: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        file.read(read_buffer)
    }

    pub fn read_at(
        file: &mut File,
        read_buffer: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        file.seek(SeekFrom::Start(offset))?;
        file.read(read_buffer)
    }

    pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        remove_file(path)
    }

    pub fn remove_dir<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        remove_dir_all(path)
    }

    pub fn metadata<P: AsRef<Path>>(path: P) -> Result<std::fs::Metadata, std::io::Error> {
        std::fs::metadata(path)
    }

    pub fn rename<F: AsRef<Path>, T: AsRef<Path>>(from: F, to: T) -> Result<(), std::io::Error> {
        std::fs::rename(from, to)
    }

    pub fn truncate<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        let file = Fs::open_file(path)?;
        file.set_len(0)
    }

    pub fn cleanup(path: &PathBuf) -> Result<(), Error> {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(ProgressStyle::default_spinner().template("{spinner} {msg}"));
        spinner.set_message(format!("clean up {}", path.to_str().ok_or(Error::Unknown("failed to convert PathBuf to String".to_string()))?));

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

    pub fn path_to_str(path: &PathBuf) -> Result<&str, Error> {
        path.as_os_str().to_str().ok_or(Error::Unknown("failed to convert PathBuf to String".to_string()))
    }

    // count the number of operations in a time window
    // the time window length is in milliseconds
    // the input times contains the timestamps in unix_time format. The first 10 digits are
    // date and time in seconds and the last 9 digits show the milliseconds
    pub fn ops_in_window(times: &Vec<SystemTime>) -> Result<Vec<Record>, Error> {
        let len = times.len();
        let first = times[0]; // first timestamp
        let last = times[len - 1]; // last timestamp

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
        }  else if duration < 50f64 {
            100
        } else if duration < 100f64 {
            150
        } else if duration < 150f64 {
            200
        } else if duration < 200f64 {
            500
        } else if duration < 300f64 {
            1000
        } else  {
            5000
        };


        let mut records = vec![];

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
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string(),
                ]
                    .to_vec(),
            };
            records.push(record);

            // go the next time window
            next = next.add(Duration::from_millis(window));
            ops = 0;
        }

        // count the remaining
        if idx < len {
            ops = len - idx;
            let time = last.duration_since(first)?.as_secs_f64();
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string(),
                ]
                    .to_vec(),
            };
            records.push(record);
        }

        Ok(records)
    }
}

