use std::path::{Path, PathBuf};
use byte_unit::Byte;
use crate::Error;

///
/// The Benchmark trait including configurations and common behaviours
///
pub trait Bench {
    fn configure<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        io_size: Option<String>,
        run_time: Option<f64>,
        workload: Option<P>,
        mount_paths: Vec<P>,
        fs_names: Vec<String>,
        log_path: P,
    ) -> Result<Self, Error> where Self: Sized {
        let config = Config::new(io_size, run_time, workload, mount_paths, fs_names, log_path)?;
        Bench::new(config)
    }

    fn new(config: Config) -> Result<Self, Error> where Self: Sized;

    fn run(&self) -> Result<(), Error>;
}

///
/// Configuration parameters
///
pub struct Config {
    pub io_size: usize,
    pub run_time: f64,
    pub workload: PathBuf,
    pub mount_paths: Vec<PathBuf>,
    pub fs_names: Vec<String>,
    pub log_path: PathBuf
}

impl Config {
    fn new<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        io_size: Option<String>, run_time: Option<f64>, workload: Option<P>, mount_paths: Vec<P>, fs_names: Vec<String>, log_path: P) -> Result<Self, Error> {
        let io_size = if let Some(io_size) = io_size {
            let io_size = Byte::from_str(io_size)?;
            io_size.get_bytes() as usize
        } else {
            4096 // the default io_size: 4 KiB
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

        let mount_paths = mount_paths.iter()
            .map(|mp| {
                let mount_path = Path::new(mp);
                PathBuf::from(mount_path)
            }).collect::<Vec<PathBuf>>();

        Ok(Self {
            io_size,
            run_time,
            workload,
            mount_paths,
            fs_names,
            log_path,
        })
    }
}