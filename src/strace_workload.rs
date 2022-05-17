use crate::format::{time_format, time_format_by_unit, time_unit};
use crate::plotter::Plotter;
use crate::{BenchResult, Error, Fs, Progress, Record, ResultMode};
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use strace_parser::{FileDir, Operation, Parser, Process};

pub struct StraceWorkloadRunner {
    mount_paths: Vec<PathBuf>,
    fs_names: Vec<String>,
    log_path: PathBuf,
    processes: Vec<Process>, // list of processes that their ops can be run concurrently
    postponed_processes: Vec<Process>, // list of processes that their ops should be run at end
    files: Vec<FileDir>,     // the files and directories accessed and logged by strace
}

impl StraceWorkloadRunner {
    pub fn new(
        mount_paths: Vec<PathBuf>,
        fs_names: Vec<String>,
        log_path: PathBuf,
        strace_path: PathBuf,
    ) -> Result<Self, Error> {
        // parse the strace log file and extract the operations
        let mut parser = Parser::new(strace_path);
        let (processes, postponed_processes) = parser.parse()?;
        let files = parser.existing_files()?;
        let mut files = Vec::from_iter(files.into_iter());

        files.retain(|file_dir| file_dir.path() != "/");

        Ok(Self {
            mount_paths,
            fs_names,
            log_path,
            processes,
            postponed_processes,
            files,
        })
    }

    pub fn replay(&mut self) -> Result<(), Error> {

        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let mount_paths = self.mount_paths.clone();
        let fs_names = self.fs_names.clone();
        for (idx, mount_path)  in mount_paths.iter().enumerate() {
            let mut base_path = mount_path.clone();
            base_path.push("strace_workload");

            let actual_behaviour_times =
                self.actual_behaviour(&base_path, &fs_names[idx], progress_style.clone())?;

            let time_unit = time_unit(actual_behaviour_times[0]);
            let header = ["op".to_string(), format!("time ({})", time_unit)].to_vec();

            // log the actual results
            let mut records = vec![];
            let mut results = BenchResult::new(header.clone());
            for (idx, actual_behaviour_time) in actual_behaviour_times.iter().enumerate() {
                records.push(Record {
                    fields: [
                        idx.to_string(),
                        time_format_by_unit(*actual_behaviour_time, time_unit)?.to_string(),
                    ]
                        .to_vec(),
                });
            }
            results.add_records(records)?;
            let mut file_name = self.log_path.clone();
            file_name.push(format!("{}_strace_workload_actual.csv", self.fs_names[idx]));
            results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(
                &file_name,
                Some("actual order".to_string()),
                &ResultMode::OpTimes,
            )?;




            let parallel_times = self.parallel(&base_path, &fs_names[idx], progress_style.clone())?;

            // log the parallel results
            let mut results = BenchResult::new(header);
            let mut records = vec![];
            for (idx, parallel_time) in parallel_times.iter().enumerate() {
                records.push(Record {
                    fields: [
                        idx.to_string(),
                        time_format_by_unit(*parallel_time, time_unit)?.to_string(),
                    ]
                        .to_vec(),
                });
            }
            results.add_records(records)?;
            let mut file_name_p = self.log_path.clone();
            file_name_p.push(format!("{}_strace_workload_parallel.csv", self.fs_names[idx]));
            results.log(&file_name_p)?;

            plotter.add_coordinates(
                &file_name_p,
                Some("parallel".to_string()),
                &ResultMode::OpTimes,
            )?;

            // plot the results
            let mut file_name = self.log_path.clone();
            file_name.push(format!("{}_strace_workload.svg", self.fs_names[idx]));
            plotter.line_chart(
                Some("Operations"),
                Some(&format!("Time ({})", time_unit)),
                None,
                false,
                false,
                &file_name,
            )?;
        }

        println!("results logged to: {}", Fs::path_to_str(&self.log_path)?);

        Ok(())
    }

    // replay the workload by mimicking the actual workload's order, e.g, if a process p1 clone
    // another process p2 in the workload to execute some operations, this runner spawns a thread
    // when it reaches the clone operation to replay p2's operations
    fn actual_behaviour(
        &mut self,
        base_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<Vec<f64>, Error> {
        self.setup(&base_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("replaying in the actual order ({})", fs_name));
        let progress = Progress::start(bar);

        let start = SystemTime::now();
        let mut times = vec![];

        // replay the operations of the first process
        // if there would be other processes, the have been cloned by the first one and their
        // operations will be replayed when the clone op is seen
        let first_process = &self.processes[0];
        for (_op_id, op) in first_process.ops() {
            let mut time = self.exec(op, &base_path)?;
            times.append(&mut time);
        }

        // replay the postponed processes' ops
        for process in self.postponed_processes.iter() {
            for (_op_id, op) in process.ops() {
                let mut time = self.exec(&op, &base_path)?;
                times.append(&mut time);
            }
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        println!("{:11} {}", "run time:", time_format(end));
        println!();
        Ok(times)
    }

    // replay the processes' operations (except the postponed operations) all in parallel
    fn parallel(
        &mut self,
        base_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<Vec<f64>, Error> {
        self.setup(&base_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("replaying in parallel ({})", fs_name));
        let progress = Progress::start(bar);

        let start = SystemTime::now();
        let mut times = vec![];

        // replay the processes' operations in parallel
        for process in self.processes.iter() {
            crossbeam::thread::scope(|s| {
                s.spawn(|_| -> Result<(), Error> {
                    for (_op_id, op) in process.ops() {
                        // ignore the clone operation as we are replaying all the operations in parallel
                        if op.name() != "Clone".to_string() {
                            let mut time = self.exec(op, &base_path)?;
                            times.append(&mut time);
                        }
                    }
                    Ok(())
                });
            })
            .unwrap();
        }

        // replay the postponed processes' ops
        for process in self.postponed_processes.iter() {
            for (_op_id, op) in process.ops() {
                let mut time = self.exec(&op, &base_path)?;
                times.append(&mut time);
            }
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        println!("{:11} {}", "run time:", time_format(end));
        println!();
        Ok(times)
    }

    // create the directory hierarchy of the workload
    pub fn setup(&mut self, base_path: &PathBuf) -> Result<(), Error> {
        Fs::cleanup(&base_path)?;

        for file_dir in self.files.iter() {
            match file_dir {
                FileDir::File(path, size) => {
                    let new_path = Fs::map_path(base_path, path)?;

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

                    let mut file = Fs::make_file(&new_path)?;
                    file.write(&mut rand_content)?;
                }
                FileDir::Dir(path, _) => {
                    let new_path = Fs::map_path(base_path, path)?;

                    // create the directory
                    if !new_path.exists() {
                        Fs::make_dir_all(&new_path)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn exec(&self, op: &Operation, base_path: &PathBuf) -> Result<Vec<f64>, Error> {
        let mut times = vec![];
        match op {
            &Operation::Mkdir(ref file, ref _mode) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::make_dir(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Mknod(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                // create a file and sets its size and offset
                let begin = SystemTime::now();
                match Fs::make_file(path) {
                    Ok(file) => {
                        file.set_len(0)?;
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Remove(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let path = PathBuf::from(path);
                let begin = SystemTime::now();
                if path.is_dir() {
                    match Fs::remove_dir(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        }
                        Err(_err) => {}
                    }
                } else {
                    match Fs::remove_file(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        }
                        Err(_err) => {}
                    }
                }
            }
            Operation::Read(ref file, ref offset, ref len) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let mut buffer = vec![0u8; *len];

                match Fs::open_file(path) {
                    Ok(mut file) => {
                        let begin = SystemTime::now();
                        match Fs::read_at(&mut file, &mut buffer, *offset as u64) {
                            Ok(_) => {
                                times.push(begin.elapsed()?.as_secs_f64());
                            }
                            Err(_err) => {}
                        }
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Write(ref file, ref offset, ref len, ref _content) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let mut rand_content = vec![0u8; *len];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_content);

                let mut file = Fs::open_file(path)?;

                let begin = SystemTime::now();
                match Fs::write_at(&mut file, &mut rand_content, *offset as u64) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::OpenAt(ref file, ref _offset) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                if path.is_file() {
                    match Fs::open_file(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        }
                        Err(_err) => {}
                    }
                } else if path.is_dir() {
                    match Fs::open_dir(path) {
                        Ok(_) => {
                            times.push(begin.elapsed()?.as_secs_f64());
                        }
                        Err(_err) => {}
                    }
                } else {
                }
            }
            &Operation::Truncate(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::truncate(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Stat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Fstat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Statx(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::StatFS(ref file) => {
                let _path = Fs::map_path(base_path, file.path()?)?;
                let _begin = SystemTime::now();
                // TODO there is no statfs in std::fs and I may need to implement it in another way?
            }
            &Operation::Fstatat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Rename(ref file, ref to) => {
                let from = Fs::map_path(base_path, file.path()?)?;
                let to = Fs::map_path(base_path, to)?;

                let begin = SystemTime::now();
                match Fs::rename(from, to) {
                    Ok(_) => {
                        times.push(begin.elapsed()?.as_secs_f64());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::GetRandom(ref len) => {
                let begin = SystemTime::now();

                let mut rand_content = vec![0u8; *len];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_content);

                times.push(begin.elapsed()?.as_secs_f64());
            }
            &Operation::Clone(pid) => {
                // get the list of operations done by the process which is going to be cloned
                match self.processes.iter().find(|p| p.pid() == pid) {
                    Some(process) => {
                        crossbeam::thread::scope(|s| {
                            s.spawn(|_| -> Result<(), Error> {
                                for (_op_id, op) in process.ops() {
                                    let mut time = self.exec(op, base_path)?;
                                    times.append(&mut time);
                                }
                                Ok(())
                            });
                        })
                        .unwrap();
                    }
                    None => {}
                }
            }
            _ => {}
        };

        Ok(times)
    }
}
