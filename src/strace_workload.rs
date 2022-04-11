use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use strace_parser::{FileDir, Operation, Parser};
use crate::{BenchResult, Error, Fs, Record, ResultMode};
use crate::data_logger::DataLogger;
use crate::plotter::Plotter;

pub struct StraceWorkloadRunner {
    // iteration: u64,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
    ops: Vec<Operation>, // the operations extracted from the strace log
    files: Vec<FileDir> // the files and directories accessed and logged by strace
}

impl StraceWorkloadRunner {
    pub fn new(
        // iteration: u64,
        mount_path: PathBuf,
        fs_name: String,
        log_path: PathBuf,
        strace_path: PathBuf,
    ) -> Result<Self, Error> {

        // parse the strace log file and extract the operations
        let mut parser = Parser::new(strace_path);
        let mut ops = parser.parse()?;
        let files = parser.accessed_files()?;
        let mut files = Vec::from_iter(files.into_iter());

        // remove no-op and stat operations
        ops.retain(|op| op != &Operation::NoOp &&
            !matches!(op, Operation::Stat(_)));

        files.retain(|file_dir| file_dir.path() != "/");

        Ok(Self {
            // iteration,
            mount_path,
            fs_name,
            log_path,
            ops,
            files
        })
    }



    pub fn replay(&mut self) -> Result<(), Error> {
        let mut base_path = self.mount_path.clone();
        base_path.push("strace_workload");

        self.setup(&base_path)?;

        let mut times = vec![];

        for op in self.ops.iter() {
            match op {
                &Operation::Mkdir(ref path, ref _mode) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::make_dir(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Mknod(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    // create a file and sets its size and offset
                    let begin = SystemTime::now();
                    match Fs::make_file(path) {
                        Ok(file) => {
                            file.set_len(0)?;
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Remove(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let path = PathBuf::from(path);
                    let begin = SystemTime::now();
                    if path.is_dir() {
                        match Fs::remove_dir(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_err) => {}
                        }
                    } else {
                        match Fs::remove_file(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_err) => {}
                        }
                    }
                }
                Operation::Read(ref path, ref offset, ref len) => {
                    let path = self.map_path(&base_path, path)?;
                    let mut buffer = vec![0u8; *len];
                    let begin = SystemTime::now();
                    match Fs::open_file(path) {
                        Ok(mut file) => {
                            match Fs::read_at(&mut file, &mut buffer, *offset as u64) {
                                Ok(_) => {
                                    times.push(begin.elapsed()?.as_secs_f64());
                                },
                                Err(_err) => {}
                            }
                        },
                        Err(_) => {}
                    }


                }
                &Operation::Write(ref path, ref offset, ref len, ref _content) => {
                    let path = self.map_path(&base_path, path)?;
                    let mut rand_content = vec![0u8; *len];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut rand_content);

                    let mut file = Fs::open_file(path)?;

                    let begin = SystemTime::now();
                    match Fs::write_at(&mut file, &mut rand_content, *offset as u64) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::OpenAt(ref path, ref _offset) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    if path.is_file() {
                        match Fs::open_file(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_err) => {}
                        }
                    } else if path.is_dir() {
                        match Fs::open_dir(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_err) => {}
                        }
                    }

                }
                &Operation::Truncate(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::truncate(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Stat(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Fstat(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Statx(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::StatFS(ref path) => {
                    let _path = self.map_path(&base_path, path)?;
                    let _begin = SystemTime::now();
                    // TODO there is no statfs in std::fs and I may need to implement it in another way?
                }
                &Operation::Fstatat(ref path) => {
                    let path = self.map_path(&base_path, path)?;
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                }
                &Operation::Rename(ref from, ref to) => {
                    let from = self.map_path(&base_path, from)?;
                    let to = self.map_path(&base_path, to)?;

                    let begin = SystemTime::now();
                    match Fs::rename(from, to) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_err) => {}
                    }
                },
                &Operation::GetRandom(ref len) => {
                    let begin = SystemTime::now();

                    let mut rand_content = vec![0u8; *len];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut rand_content);

                    times.push(begin.elapsed()?.as_secs_f64());
                },
                &Operation::NoOp => {}
            }
        }

        println!("ops len: {}", self.ops.len());
        println!("times len: {}", times.len());

        // plot and log the results
        let header = ["op".to_string(), "time".to_string()].to_vec();
        let op_time: Vec<_> = (0..).into_iter().zip(times.into_iter()).collect();
        let mut results = BenchResult::new(header);
        let mut records = vec![];
        for (op, time) in op_time {
            records.push(Record {
                fields: [op.to_string(), time.to_string()].to_vec(),
            });
        }
        results.add_records(records)?;

        let logger = DataLogger::new(self.fs_name.clone(), self.log_path.clone())?;
        let workload_log = logger.log(results, "strace_workload")?;

        let plotter = Plotter::parse(PathBuf::from(workload_log), &ResultMode::OpTimes)?;
        plotter.line_chart(
            Some("Operations"),
            Some("Time [ns]"),
            None,
            false,
            false,
        )?;

        Ok(())
    }

    // create the directory hierarchy of the workload
    pub fn setup(&mut self, base_path: &PathBuf) -> Result<(), Error> {
        // let mut root_path = self.mount_path.clone();
        // root_path.push("strace_workload");
        Fs::cleanup(&base_path)?;

        let style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");
        let bar = ProgressBar::new(self.files.len() as u64);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "setup paths"));


        for file_dir in self.files.iter() {

            match file_dir {
                FileDir::File(path, size) => {
                    let new_path = self.map_path(base_path, path)?;

                    // remove the file name from the path
                    let mut parents = new_path.clone();
                    parents.pop();

                    // create the parent directory hierarchy
                    if !parents.exists() {
                        Fs::make_dir_all(&parents)?;
                    }

                    // create the file and fill it with random content
                    let mut rand_content = vec![0u8; *size];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut rand_content);

                    let mut file  = Fs::make_file(&new_path)?;
                    file.write(&mut rand_content)?;
                },
                FileDir::Dir(path, _) => {
                    let new_path = self.map_path(base_path, path)?;

                    // create the directory
                    if !new_path.exists() {
                        Fs::make_dir_all(&new_path)?;
                    }
                }
            }

            bar.inc(1);
        }

        bar.finish_and_clear();

        Ok(())
    }

    // change the paths to a path relative to our workload path
    fn map_path(&self, base_path: &PathBuf, path: &str) -> Result<PathBuf, Error> {
        let mut new_path = base_path.clone();

        let mut path = path.to_string();
        if path.starts_with("/") {
            path = path[1..].to_string();
        }
        new_path.push(path);

        Ok(new_path)
    }
}