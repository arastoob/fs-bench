use std::path::Path;
use byte_unit::Byte;
use crate::{BenchMode, BenchResult, Record, Error, make_dir, make_file, write_file, read_file};
use crate::data_logger::DataLogger;
use std::fs::remove_dir_all;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use rand::{thread_rng, Rng, RngCore};

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
                // remove the existing files/directories from the previous runs
                self.cleanup()?;

                let header = ["operation".to_string(), "runtime(s)".to_string(), "ops/s".to_string()].to_vec();
                let mut results = BenchResult::new(header);

                results.add_record(self.mkdir()?)?;
                results.add_record(self.mknod()?)?;
                results.add_record(self.read()?)?;
                results.add_record(self.write()?)?;

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

    fn read(&self) -> Result<Record, Error> {
        println!("read benchmark...");
        let (mount_path, _) = self.mount_path.rsplit_once("/").unwrap(); // remove / at the end
        let root_path = format!("{}/{}", mount_path, "read");

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        println!("pre-allocating...");
        let size = self.io_size;
        for file in 1..1001 {
            let file_name = format!("{}/{}", root_path, file);
            let mut file = make_file(&file_name)?;

            // generate a buffer of size write_size filled with random integer values
            let mut rand_buffer = vec![0u8; size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_buffer);

            file.write(&rand_buffer)?;
        }

        let timer = timer::Timer::new();
        // Number of times the callback has been called.
        let count = Arc::new(Mutex::new(0));

        // Start repeating. Each callback increases `count`.
        let mut read_buffer = vec![0u8; size];
        let guard = {
            let count = count.clone();

            timer.schedule_repeating(chrono::Duration::milliseconds(0), move || {
                let file = thread_rng().gen_range(1..1001);
                let file_name = format!("{}/{}", root_path, file);

                match read_file(&file_name, &mut read_buffer) {
                    Ok(_) => {
                        *count.lock().unwrap() += 1;
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

        println!("{:?} read ops in {} seconds", count_result, self.runtime);
        println!("ops/s: {:?}\n", count_result / self.runtime);

        let record = Record {
            fields: ["read".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn write(&self) -> Result<Record, Error> {
        println!("write benchmark...");
        let (mount_path, _) = self.mount_path.rsplit_once("/").unwrap(); // remove / at the end
        let root_path = format!("{}/{}", mount_path, "write");

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        println!("pre-allocation...");
        for file in 1..1001 {
            let file_name = format!("{}/{}", root_path, file);
            make_file(&file_name).expect("pre-allocation failed.");
        }

        // create a big vector filled with random content
        let size = self.io_size;
        let mut rand_content = vec![0u8; 8192 * size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_content);

        let timer = timer::Timer::new();
        // Number of times the callback has been called.
        let count = Arc::new(Mutex::new(0));

        // Start repeating. Each callback increases `count`.
        let guard = {
            let count = count.clone();

            timer.schedule_repeating(chrono::Duration::milliseconds(0), move || {
                let rand_content_index =
                    thread_rng().gen_range(0..(8192 * size) - size - 1);
                let mut content =
                    rand_content[rand_content_index..(rand_content_index + size)].to_vec();

                let file = thread_rng().gen_range(1..1001);
                let file_name = format!("{}/{}", root_path, file);

                match write_file(&file_name, &mut content) {
                    Ok(_) => {
                        *count.lock().unwrap() += 1;
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

        println!("{:?} write ops in {} seconds", count_result, self.runtime);
        println!("ops/s: {:?}\n", count_result / self.runtime);

        let record = Record {
            fields: ["write".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn cleanup(&self) -> Result<(), Error> {
        println!("cleanup...");
        let (mount_path, _) = self.mount_path.rsplit_once("/").unwrap(); // remove / at the end

        let mkdir_path = format!("{}/{}", mount_path, "mkdir");
        let mkdir_path = Path::new(&mkdir_path);
        if mkdir_path.exists() {
            remove_dir_all(mkdir_path)?;
        }

        let mknod_path = format!("{}/{}", mount_path, "mknod");
        let mknod_path = Path::new(&mknod_path);
        if mknod_path.exists() {
            remove_dir_all(mknod_path)?;
        }

        let read_path = format!("{}/{}", mount_path, "read");
        let read_path = Path::new(&read_path);
        if read_path.exists() {
            remove_dir_all(read_path)?;
        }

        let write_path = format!("{}/{}", mount_path, "write");
        let write_path = Path::new(&write_path);
        if write_path.exists() {
            remove_dir_all(write_path)?;
        }

        Ok(())
    }
}