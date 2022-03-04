use std::path::{Path, PathBuf};
use byte_unit::Byte;
use crate::{BenchMode, BenchResult, Record, Error, make_dir, make_file, write_file, read_file};
use crate::data_logger::DataLogger;
use std::fs::remove_dir_all;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;
use rand::{thread_rng, Rng, RngCore};
use indicatif::{ProgressBar, ProgressStyle};
use crate::plotter::Plotter;

#[derive(Debug)]
pub struct MicroBench {
    mode: BenchMode,
    runtime: u16,
    io_size: usize,
    iteration: Option<u64>,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf
}



impl MicroBench {
    pub fn new(mode: BenchMode, runtime: u16, io_size: String, iteration: Option<u64>, mount_path: PathBuf, fs_name: String, log_path: PathBuf) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            mode,
            runtime,
            io_size,
            iteration,
            mount_path,
            fs_name,
            log_path
        })
    }

    pub fn run(&self) -> Result<(), Error> {

        let progress_style = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {msg} {bar:40.cyan/blue}")
            .progress_chars("##-");

        let logger = DataLogger::new(self.fs_name.clone(), self.log_path.clone())?;

        match self.mode {
            BenchMode::OpsPerSecond => {
                let header = ["operation".to_string(), "runtime(s)".to_string(), "ops/s".to_string()].to_vec();
                let mut results = BenchResult::new(header);

                results.add_record(self.mkdir(progress_style.clone())?)?;
                results.add_record(self.mknod(progress_style.clone())?)?;
                results.add_record(self.read(progress_style.clone())?)?;
                results.add_record(self.write(progress_style)?)?;

                let log_file_name = logger.log(results, &self.mode)?;

                let plotter = Plotter::parse(log_file_name.clone(), &self.mode)?;
                plotter.bar_chart(Some("Operation"), Some("Ops/s"), None)?;
                println!("results logged to {}", path_to_str(&self.log_path));
            },
            BenchMode::Throughput => {}
            BenchMode::Behaviour => {}
        }

        Ok(())
    }


    fn mkdir(&self, style: ProgressStyle) -> Result<Record, Error> {
        self.cleanup("mkdir")?;
        let root_path = format!("{}/{}", path_to_str(&self.mount_path), "mkdir");

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
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            })
        };

        let bar = ProgressBar::new((self.runtime as u64) * 10);
        bar.set_style(style);
        bar.set_message(format!("{:10}", "mkdir"));

        for _ in 0..(self.runtime as u64) * 10 {
            bar.inc(1);
            thread::sleep(Duration::from_millis(100));
        }
        bar.finish();

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        let record = Record {
            fields: ["mkdir".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn mknod(&self, style: ProgressStyle) -> Result<Record, Error> {
        self.cleanup("mknod")?;
        let root_path = format!("{}/{}", path_to_str(&self.mount_path), "mknod");

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

        let bar = ProgressBar::new((self.runtime as u64) * 10);
        bar.set_style(style);
        bar.set_message(format!("{:10}", "mknod"));

        for _ in 0..(self.runtime as u64) * 10 {
            bar.inc(1);
            thread::sleep(Duration::from_millis(100));
        }
        bar.finish();

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        let record = Record {
            fields: ["mknod".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn read(&self, style: ProgressStyle) -> Result<Record, Error> {
        self.cleanup("read")?;
        let root_path = format!("{}/{}", path_to_str(&self.mount_path), "read");

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        // println!("pre-allocating...");
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

        let bar = ProgressBar::new((self.runtime as u64) * 10);
        bar.set_style(style);
        bar.set_message(format!("{:10}", "read"));

        for _ in 0..(self.runtime as u64) * 10 {
            bar.inc(1);
            thread::sleep(Duration::from_millis(100));
        }
        bar.finish();

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        let record = Record {
            fields: ["read".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn write(&self, style: ProgressStyle) -> Result<Record, Error> {
        self.cleanup("write")?;
        let root_path = format!("{}/{}", path_to_str(&self.mount_path), "write");

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        // println!("pre-allocation...");
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

        let bar = ProgressBar::new((self.runtime as u64) * 10);
        bar.set_style(style);
        bar.set_message(format!("{:10}", "write"));

        for _ in 0..(self.runtime as u64) * 10 {
            bar.inc(1);
            thread::sleep(Duration::from_millis(100));
        }
        bar.finish();

        // Now drop the guard. This should stop the timer.
        drop(guard);

        let count_result = *count.lock().unwrap();

        let record = Record {
            fields: ["write".to_string(), self.runtime.to_string(), (count_result / self.runtime).to_string()].to_vec()
        };

        Ok(record)
    }

    fn cleanup(&self, bench_name: &str) -> Result<(), Error> {
        let bench_name = bench_name.to_string();
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(ProgressStyle::default_spinner()
            .template("{msg} {spinner}"));
        spinner.set_message(format!("{} clean up", bench_name));


        let (sender, receiver) = channel();
        let mount_path = path_to_str(&self.mount_path).to_string();
        thread::spawn(move || {
            let path = format!("{}/{}", mount_path, bench_name);
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
                    spinner.finish_and_clear();
                    break;
                },
                _ => {
                    thread::sleep(Duration::from_millis(50));
                    spinner.inc(1);
                }
            }
        }

        Ok(())
    }
}

fn path_to_str(path: &PathBuf) -> &str {
    path.as_os_str().to_str().unwrap()
}