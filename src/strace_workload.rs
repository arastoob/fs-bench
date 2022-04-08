use std::collections::HashSet;
use std::ffi::OsStr;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use strace_parser::{FileDir, Operation, Parser};
use crate::{Error, Fs};

pub struct StraceWorkloadRunner {
    iteration: u64,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
    pub ops: Vec<Operation>, // the operations extracted from the strace log
    pub files: Vec<FileDir> // the files and directories accessed and logged by strace
}

impl StraceWorkloadRunner {
    pub fn new(
        iteration: u64,
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
            iteration,
            mount_path,
            fs_name,
            log_path,
            ops,
            files
        })
    }



    pub fn replay(&mut self) -> Result<(), Error> {
        self.setup()?;

        let mut times = vec![];

        for op in self.ops.iter() {
            println!("{}", op);

            match op {
                &Operation::Mkdir(ref path, ref _mode) => {
                    let begin = SystemTime::now();
                    match Fs::make_dir(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Mknod(ref path, ref offset, ref size) => {
                    // create a file and sets its size and offset
                    let begin = SystemTime::now();
                    match Fs::make_file(path) {
                        Ok(mut file) => {
                            file.set_len(*size as u64);
                            file.seek(SeekFrom::Start(*offset as u64));
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Remove(ref path) => {
                    let path = PathBuf::from(path);
                    let begin = SystemTime::now();
                    if path.is_dir() {
                        match Fs::remove_dir(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_) => {}
                        }
                    } else {
                        match Fs::remove_file(path) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            },
                            Err(_) => {}
                        }
                    }
                }
                Operation::Read(ref path, ref offset, ref len) => {
                    let mut buffer = vec![0u8; *len];
                    let mut file = Fs::open_file(path)?;

                    let begin = SystemTime::now();
                    match Fs::read_at(&mut file, &mut buffer, *offset as u64) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Write(ref path, ref offset, ref len, ref _content) => {
                    let mut rand_content = vec![0u8; *len];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut rand_content);

                    let mut file = Fs::open_file(path)?;

                    let begin = SystemTime::now();
                    match Fs::write_at(&mut file, &mut rand_content, *offset as u64) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::OpenAt(ref path, ref offset) => {
                    let begin = SystemTime::now();
                    match Fs::open_file(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Stat(ref path) => {
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Fstat(ref path) => {
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Statx(ref path) => {
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::StatFS(ref path) => {
                    let begin = SystemTime::now();
                }
                &Operation::Fstatat(ref path) => {
                    let begin = SystemTime::now();
                    match Fs::metadata(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
                    }
                }
                &Operation::Rename(ref old_name, ref new_name) => {
                    // make sure the paths are within the mount_path
                    let mut from = PathBuf::from(old_name);
                    let mut to = PathBuf::from(new_name);
                    if !old_name.contains(Fs::path_to_str(&self.mount_path)?) {
                        let mut name = self.mount_path.clone();
                        name.push("strace_workload");
                        name.push(old_name);
                        from = name;
                    }

                    if !new_name.contains(Fs::path_to_str(&self.mount_path)?) {
                        let mut name = self.mount_path.clone();
                        name.push("strace_workload");
                        name.push(new_name);
                        to = name;
                    }

                    let begin = SystemTime::now();
                    match Fs::rename(from, to) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        },
                        Err(_) => {}
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

        Ok(())
    }

    // create the directory hierarchy of the workload
    pub fn setup(&mut self) -> Result<(), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("strace_workload");
        Fs::cleanup(&root_path)?;

        let style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let bar = ProgressBar::new(self.files.len() as u64);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "setup paths"));

        for file_dir in self.files.iter_mut() {

            // change the files paths to a path relative to our workload path
            let mut new_path = root_path.clone();
            let mut fd_path = file_dir.path().to_string();
            if fd_path.starts_with("/") {
                fd_path = fd_path[1..].to_string();
            }
            new_path.push(fd_path);

            // update the path in the operations vector
            for op in self.ops.iter_mut() {
                if op.path() == file_dir.path() {
                    op.update_path(Fs::path_to_str(&new_path)?);
                }
            }

            match file_dir {
                FileDir::File(path, size) => {
                    // update the new path in the files
                    *path = Fs::path_to_str(&new_path)?.to_string();

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
                    // update the new path in the files
                    *path = Fs::path_to_str(&new_path)?.to_string();

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
}