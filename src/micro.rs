use std::path::Path;
use byte_unit::Byte;
use crate::{BenchMode, BenchResult, Record, Error, make_dir, make_file};
use crate::data_logger::DataLogger;
use std::fs::remove_dir_all;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug)]
pub struct MicroBench {
    mode: BenchMode,
    runtime: u16,
    io_size: usize,
    iteration: Option<u64>,
    mount_path: String,
    logger: DataLogger
}



impl MicroBench {
    pub fn new(mode: BenchMode, runtime: u16, io_size: String, iteration: Option<u64>, mount_path: String, fs_name: String, log_path: String) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        let logger = DataLogger::new(fs_name, log_path)?;

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
                let header = ["operation".to_string(), "runtime".to_string(), "ops/s".to_string()].to_vec();
                let mut results = BenchResult::new(header);

                results.add_record(self.mkdir()?)?;
                results.add_record(self.mknod()?)?;

                let log_file_name = self.logger.log(results)?;

                println!("results logged to {}\n", log_file_name);
            },
            BenchMode::Throughput => {}
            BenchMode::Behaviour => {}
        }

        Ok(())
    }


    fn mkdir(&self) -> Result<Record, Error> {
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
        println!("ops/s: {:?}\n", count_result / self.runtime);

        let record = Record {
            fields: ["mkdir".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }


    fn mknod(&self) -> Result<Record, Error> {
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
        println!("ops/s: {:?}\n", count_result / self.runtime);

        let record = Record {
            fields: ["mknod".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }
}