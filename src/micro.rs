use std::path::Path;
use byte_unit::Byte;
use crate::{BenchMode, Error, make_dir, make_file};
use crate::data_logger::DataLogger;
use std::fs::remove_dir_all;
use std::sync::{Arc, Mutex};
use std::thread;
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct MicroBench {
    mode: BenchMode,
    runtime: u16,
    io_size: usize,
    iteration: Option<u64>,
    mount_path: String,
    logger: DataLogger
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpsPerSecondResult {
    pub runtime: u16,
    pub ops: u64,
    pub ops_per_second: f64
}

impl OpsPerSecondResult {
    pub fn new(runtime: u16, ops: u64, ops_per_second: f64) -> Self {
        Self {
            runtime,
            ops,
            ops_per_second
        }
    }
}

impl MicroBench {
    pub fn new(mode: BenchMode, runtime: u16, io_size: String, iteration: Option<u64>, mount_path: String, logger: DataLogger) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            mode,
            runtime,
            io_size,
            iteration,
            mount_path,
            logger
        })
    }

    pub fn run(&self) -> Result<(), Error> {

        match self.mode {
            BenchMode::OpsPerSecond => {
                let mkdir_results = self.mkdir()?;
                let mkdir_log_path = self.logger.log("mkdir", mkdir_results)?;
                println!("results logged to {}\n", mkdir_log_path);

                let mknod_results = self.mknod()?;
                let mknod_log_path = self.logger.log("mknod", mknod_results)?;
                println!("results logged to {}\n", mknod_log_path);
            },
            BenchMode::Throughput => {}
            BenchMode::Behaviour => {}
        }

        Ok(())
    }


    fn mkdir(&self) -> Result<OpsPerSecondResult, Error> {
        println!("mkdir benchmark...");
        let (mount_path, _) = self.mount_path.rsplit_once("/").unwrap(); // remove / at the end
        let root_path = format!("{}/{}", mount_path, "mkdir");

        let root_dir_path = Path::new(&root_path);
        // remove the directory if exist
        if root_dir_path.exists() {
            println!("path {} already exist, removing...", root_path);
            remove_dir_all(root_dir_path).expect("Removing the existing directory failed");
        }

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        let mut dir = 0;

        let timer = timer::Timer::new();
        // Number of times the callback has been called.
        let count = Arc::new(Mutex::new(0));

        // Start repeating. Each callback increases `count`.
        let guard = {
            let count = count.clone();

            timer.schedule_repeating(chrono::Duration::milliseconds(0), move || {
                let dir_name = format!("{}/{}", root_path, dir);
                match make_dir(&dir_name) {
                    Ok(()) => {
                        *count.lock().unwrap() += 1;
                        dir = dir + 1;
                        //let path = Path::new(&dir_name);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            })
        };

        // Sleep running_time seconds
        thread::sleep(std::time::Duration::new(self.runtime as u64, 0));

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        println!("{:?} mkdir ops in {} seconds", count_result, self.runtime);
        println!("ops/s: {:?}", count_result / self.runtime);

        let results = OpsPerSecondResult::new(self.runtime, count_result as u64, (count_result / self.runtime) as f64);
        Ok(results)
    }


    fn mknod(&self) -> Result<OpsPerSecondResult, Error> {
        println!("mknod benchmark...");
        let (mount_path, _) = self.mount_path.rsplit_once("/").unwrap(); // remove / at the end
        let root_path = format!("{}/{}", mount_path, "mknod");

        let root_dir_path = Path::new(&root_path);
        // remove the directory if exist
        if root_dir_path.exists() {
            println!("path {} already exist, removing...", root_path);
            remove_dir_all(root_dir_path).expect("Removing the existing directory failed");
        }

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        let mut file = 0;

        let timer = timer::Timer::new();
        // Number of times the callback has been called.
        let count = Arc::new(Mutex::new(0));

        // Start repeating. Each callback increases `count`.
        let guard = {
            let count = count.clone();

            timer.schedule_repeating(chrono::Duration::milliseconds(0), move || {
                let file_name = format!("{}/{}", root_path, file);
                match make_file(&file_name) {
                    Ok(_) => {
                        *count.lock().unwrap() += 1;
                        file = file + 1;
                        //let path = Path::new(&dir_name);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            })
        };

        // Sleep running_time seconds
        thread::sleep(std::time::Duration::new(self.runtime as u64, 0));

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        println!("{:?} mknod ops in {} seconds", count_result, self.runtime);
        println!("ops/s: {:?}", count_result / self.runtime);

        let results = OpsPerSecondResult::new(self.runtime, count_result as u64, (count_result / self.runtime) as f64);
        Ok(results)
    }
}