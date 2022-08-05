pub mod error;
mod format;
pub mod fs;
pub mod micro;
pub mod plotter;
mod progress;
pub mod stats;
pub mod trace_workload;

use crate::error::Error;
use crate::micro::BenchFn;
use byte_unit::Byte;
use std::fmt::{Display, Formatter};
use std::fs::{remove_file, OpenOptions};
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

///
/// The Benchmark trait including configurations and common behaviours
///
pub trait Bench {
    fn configure<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        io_size: Option<String>,
        file_size: Option<String>,
        fileset_size: Option<usize>,
        run_time: Option<f64>,
        workload: Option<P>,
        mount_paths: Vec<P>,
        fs_names: Vec<String>,
        log_path: P,
        parallelism_degree: Option<usize>,
    ) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let config = Config::new(
            io_size,
            file_size,
            fileset_size,
            run_time,
            workload,
            mount_paths,
            fs_names,
            log_path,
            parallelism_degree,
        )?;
        Bench::new(config)
    }

    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    fn setup(&self, path: &PathBuf, invalidate_cache: bool) -> Result<(), Error>;

    fn run(&self, bench_fn: Option<BenchFn>) -> Result<(), Error>;
}

///
/// Configuration parameters
///
pub struct Config {
    pub io_size: usize,
    pub file_size: usize,    // the file's size in the fileset
    pub fileset_size: usize, // number of files in the fileset
    pub run_time: f64,
    pub warmup_time: u64,
    pub workload: PathBuf,
    pub mount_paths: Vec<PathBuf>,
    pub fs_names: Vec<String>,
    pub log_path: PathBuf,
    pub parallelism_degree: usize,
}

impl Config {
    fn new<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        io_size: Option<String>,
        file_size: Option<String>,
        fileset_size: Option<usize>,
        run_time: Option<f64>,
        workload: Option<P>,
        mount_paths: Vec<P>,
        fs_names: Vec<String>,
        log_path: P,
        parallelism_degree: Option<usize>,
    ) -> Result<Self, Error> {
        let io_size = if let Some(io_size) = io_size {
            let io_size = Byte::from_str(io_size)?;
            io_size.get_bytes() as usize
        } else {
            4096 // the default io_size: 4 KiB
        };

        let file_size = if let Some(file_size) = file_size {
            let file_size = Byte::from_str(file_size)?;
            file_size.get_bytes() as usize
        } else {
            // 1024 * 1024 * 10 // 10 MiB
            4096
        };

        if io_size > file_size {
            return Err(Error::InvalidConfig(format!(
                "The file size ({}) cannot be smaller than the io size ({})",
                file_size, io_size
            )));
        }

        let fileset_size = if let Some(fileset_size) = fileset_size {
            fileset_size
        } else {
            // 1000 // the default fileset_size: 1000
            10_000
        };

        let parallelism_degree = if let Some(parallelism_degree) = parallelism_degree {
            parallelism_degree
        } else {
            // the default parallelism_degree: 4
            4
        };

        let run_time = if let Some(run_time) = run_time {
            run_time
        } else {
            60.0 // the default run_time: 60 seconds
        };

        let workload = if let Some(workload) = workload {
            let workload = Path::new(&workload);
            PathBuf::from(workload)
        } else {
            PathBuf::new()
        };

        let log_path = Path::new(&log_path);
        let log_path = PathBuf::from(log_path);

        let mount_paths = mount_paths
            .iter()
            .map(|mp| {
                let mount_path = Path::new(mp);
                PathBuf::from(mount_path)
            })
            .collect::<Vec<PathBuf>>();

        Ok(Self {
            io_size,
            file_size,
            fileset_size,
            run_time,
            warmup_time: 5,
            workload,
            mount_paths,
            fs_names,
            log_path,
            parallelism_degree,
        })
    }
}

///
/// Benchmark modes supported by fs-bench
///
#[derive(Debug)]
pub enum BenchMode {
    Static,
    RealTime,
    Trace,
    Throughput,
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "static" => Ok(BenchMode::Static),
            "realtime" => Ok(BenchMode::RealTime),
            "trace" => Ok(BenchMode::Trace),
            "throughput" => Ok(BenchMode::Throughput),
            _ => Err("valid benckmark modes are: static, realtime, trace, throughput".to_string()),
        }
    }
}

impl Display for BenchMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchMode::Static => write!(f, "static"),
            BenchMode::RealTime => write!(f, "realtime"),
            BenchMode::Trace => write!(f, "trace"),
            BenchMode::Throughput => write!(f, "throughput"),
        }
    }
}

///
/// Results modes generated by the fs-bench benchmarks
///
#[derive(Debug, PartialEq)]
pub enum ResultMode {
    OpsPerSecond,
    Throughput,
    Behaviour,
    OpTimes,
    SampleOpsPerSecond,
    AccumulatedTimes,
}

impl FromStr for ResultMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ops_per_second" => Ok(ResultMode::OpsPerSecond),
            "throughput" => Ok(ResultMode::Throughput),
            "behaviour" => Ok(ResultMode::Behaviour),
            "op_times" => Ok(ResultMode::OpTimes),
            "sample_ops_per_second" => Ok(ResultMode::SampleOpsPerSecond),
            "accumulated_times" => Ok(ResultMode::AccumulatedTimes),
            _ => Err("valid result modes are: ops_per_second, throughput, behaviour".to_string()),
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
            ResultMode::SampleOpsPerSecond => write!(f, "sample_ops_per_second"),
            ResultMode::AccumulatedTimes => write!(f, "accumulated_times"),
        }
    }
}

///
/// The benchmark results including the header row and the data records
///
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
        if !records.is_empty() {
            if records[0].fields.len() != self.header.len() {
                return Err(Error::Unknown(
                    "the records and header should be of the same length".to_string(),
                ));
            }
            for record in records {
                self.records.push(record);
            }
        }

        Ok(())
    }

    ///
    /// Log the bench results to the specified path
    ///
    pub fn log<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        &self,
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
        writer.write_record(&self.header)?;
        for record in self.records.iter() {
            writer.write_record(&record.fields)?;
        }

        writer.flush()?;

        Ok(())
    }
}

///
/// A record of data
///
#[derive(Debug, Clone)]
pub struct Record {
    pub fields: Vec<String>,
}

impl From<Vec<String>> for Record {
    fn from(fields: Vec<String>) -> Self {
        Self { fields }
    }
}

impl Record {
    ///
    /// count the number of operations in a time window
    /// the time window length is in milliseconds
    /// the input times contains the timestamps in unix_time format. The first 10 digits are
    /// date and time in seconds and the last 9 digits show the milliseconds
    ///
    pub fn ops_in_window(
        times: &Vec<SystemTime>,
        duration: Duration,
    ) -> Result<Vec<Record>, Error> {
        let len = times.len();
        let first = times[0]; // first timestamp
        let mut last = times[len - 1]; // last timestamp
        if last.duration_since(first)? > duration {
            last = first.add(duration);
        }

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
        } else if duration < 50f64 {
            100
        } else if duration < 100f64 {
            150
        } else if duration < 150f64 {
            200
        } else if duration < 200f64 {
            500
        } else if duration < 300f64 {
            1000
        } else {
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
