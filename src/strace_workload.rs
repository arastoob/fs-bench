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

        for (idx, mount_path) in mount_paths.iter().enumerate() {
            let mut base_path = mount_path.clone();
            base_path.push("strace_workload");

            let (
                op_times_records_actual,
                accumulated_times_records_actual,
                op_time_unit,
                accumulated_time_unit,
            ) = self.actual_behaviour(&base_path, &fs_names[idx], progress_style.clone())?;

            let op_times_header = ["op".to_string(), format!("time ({})", op_time_unit)].to_vec();
            let accumulated_times_header = [
                format!("time ({})", accumulated_time_unit),
                "ops".to_string(),
            ]
            .to_vec();

            // log the actual results
            let mut results = BenchResult::new(op_times_header.clone());
            results.add_records(op_times_records_actual)?;
            let mut file_name = self.log_path.clone();
            file_name.push(format!(
                "{}_op_times_strace_workload_actual.csv",
                self.fs_names[idx]
            ));
            results.log(&file_name)?;

            let mut op_times_plotter = Plotter::new();
            op_times_plotter.add_coordinates(
                &file_name,
                Some("actual order".to_string()),
                &ResultMode::OpTimes,
            )?;

            let mut accumulated_times_results_actual =
                BenchResult::new(accumulated_times_header.clone());
            accumulated_times_results_actual.add_records(accumulated_times_records_actual)?;
            let mut file_name = self.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times_workload_actual.csv",
                self.fs_names[idx]
            ));
            accumulated_times_results_actual.log(&file_name)?;

            let mut accumulated_times_plotter = Plotter::new();
            accumulated_times_plotter.add_coordinates(
                &file_name,
                Some("actual".to_string()),
                &ResultMode::Behaviour,
            )?;

            let (
                op_times_records_parallel,
                accumulated_times_records_parallel,
                _op_time_unit,
                _accumulated_time_unit,
            ) = self.parallel(&base_path, &fs_names[idx], progress_style.clone())?;

            // log the parallel results
            let mut results = BenchResult::new(op_times_header.clone());
            results.add_records(op_times_records_parallel)?;
            let mut file_name_p = self.log_path.clone();
            file_name_p.push(format!(
                "{}_op_times_strace_workload_parallel.csv",
                self.fs_names[idx]
            ));
            results.log(&file_name_p)?;
            op_times_plotter.add_coordinates(
                &file_name_p,
                Some("parallel".to_string()),
                &ResultMode::OpTimes,
            )?;

            let mut accumulated_times_results_parallel =
                BenchResult::new(accumulated_times_header.clone());
            accumulated_times_results_parallel.add_records(accumulated_times_records_parallel)?;
            let mut file_name = self.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times_workload_parallel.csv",
                self.fs_names[idx]
            ));
            accumulated_times_results_parallel.log(&file_name)?;
            accumulated_times_plotter.add_coordinates(
                &file_name,
                Some("parallel".to_string()),
                &ResultMode::Behaviour,
            )?;

            // plot the results
            let mut file_name = self.log_path.clone();
            file_name.push(format!(
                "{}_op_times_strace_workload.svg",
                self.fs_names[idx]
            ));
            op_times_plotter.line_chart(
                Some("Operations"),
                Some(&format!("Time ({})", op_time_unit)),
                Some(&format!("Operation times from replayed logs ({})", self.fs_names[idx])),
                false,
                false,
                &file_name,
            )?;

            // plot the accumulated results
            let mut file_name = self.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times_workload_actual.svg",
                self.fs_names[idx]
            ));
            accumulated_times_plotter.line_chart(
                Some(&format!("Time ({})", accumulated_time_unit)),
                Some("Operations"),
                Some(&format!("Accumulated times from replayed logs ({})", self.fs_names[idx])),
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
    ) -> Result<(Vec<Record>, Vec<Record>, String, String), Error> {
        self.setup(&base_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("replaying in the actual order ({})", fs_name));
        let progress = Progress::start(bar);

        let start = SystemTime::now();
        let mut op_times = vec![];
        let mut accumulated_times = vec![];

        // replay the operations of the first process
        // if there would be other processes, the have been cloned by the first one and their
        // operations will be replayed when the clone op is seen
        let first_process = &self.processes[0];
        for (_op_id, op) in first_process.ops() {
            let (mut time, mut behaviour) = self.exec(op, &base_path)?;
            op_times.append(&mut time);
            accumulated_times.append(&mut behaviour);
        }

        // replay the postponed processes' ops
        for process in self.postponed_processes.iter() {
            for (_op_id, op) in process.ops() {
                let (mut time, mut behaviour) = self.exec(&op, &base_path)?;
                op_times.append(&mut time);
                accumulated_times.append(&mut behaviour);
            }
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        // let behaviour_records = Fs::ops_in_window(&behaviours, Duration::from_secs_f64(end))?;

        let mut op_times_records = vec![];
        let op_time_unit = time_unit(op_times[0]);
        for (idx, time) in op_times.iter().enumerate() {
            op_times_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, op_time_unit)?.to_string(),
                ]
                .to_vec(),
            });
        }

        let mut accumulated_times_records = vec![];
        let first = accumulated_times[0];
        let last = accumulated_times[accumulated_times.len() - 1];
        let accumulated_time_unit = time_unit(last.duration_since(first)?.as_secs_f64());
        accumulated_times_records.push(Record {
            fields: ["0".to_string(), "0".to_string()].to_vec(),
        });
        for (idx, system_time) in accumulated_times.iter().enumerate() {
            accumulated_times_records.push(Record {
                fields: [
                    time_format_by_unit(
                        system_time.duration_since(first)?.as_secs_f64(),
                        accumulated_time_unit,
                    )?
                    .to_string(),
                    idx.to_string(),
                ]
                .to_vec(),
            });
        }

        println!("{:11} {}", "run time:", time_format(end));
        println!();
        Ok((
            op_times_records,
            accumulated_times_records,
            op_time_unit.to_string(),
            accumulated_time_unit.to_string(),
        ))
    }

    // replay the processes' operations (except the postponed operations) all in parallel
    fn parallel(
        &mut self,
        base_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<(Vec<Record>, Vec<Record>, String, String), Error> {
        self.setup(&base_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("replaying in parallel ({})", fs_name));
        let progress = Progress::start(bar);

        let start = SystemTime::now();
        let mut op_times = vec![];
        let mut accumulated_times = vec![];

        // replay the processes' operations in parallel
        for process in self.processes.iter() {
            crossbeam::thread::scope(|s| {
                s.spawn(|_| -> Result<(), Error> {
                    for (_op_id, op) in process.ops() {
                        // ignore the clone operation as we are replaying all the operations in parallel
                        if op.name() != "Clone".to_string() {
                            let (mut time, mut behaviour) = self.exec(op, &base_path)?;
                            op_times.append(&mut time);
                            accumulated_times.append(&mut behaviour);
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
                let (mut time, mut behaviour) = self.exec(&op, &base_path)?;
                op_times.append(&mut time);
                accumulated_times.append(&mut behaviour);
            }
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        let mut op_times_records = vec![];
        let op_time_unit = time_unit(op_times[0]);
        for (idx, time) in op_times.iter().enumerate() {
            op_times_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, op_time_unit)?.to_string(),
                ]
                .to_vec(),
            });
        }

        let mut accumulated_times_records = vec![];
        let first = accumulated_times[0];
        let last = accumulated_times[accumulated_times.len() - 1];
        let accumulated_time_unit = time_unit(last.duration_since(first)?.as_secs_f64());
        accumulated_times_records.push(Record {
            fields: ["0".to_string(), "0".to_string()].to_vec(),
        });
        for (idx, system_time) in accumulated_times.iter().enumerate() {
            accumulated_times_records.push(Record {
                fields: [
                    time_format_by_unit(
                        system_time.duration_since(first)?.as_secs_f64(),
                        accumulated_time_unit,
                    )?
                    .to_string(),
                    idx.to_string(),
                ]
                .to_vec(),
            });
        }

        println!("{:11} {}", "run time:", time_format(end));
        println!();
        Ok((
            op_times_records,
            accumulated_times_records,
            op_time_unit.to_string(),
            accumulated_time_unit.to_string(),
        ))
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

    fn exec(
        &self,
        op: &Operation,
        base_path: &PathBuf,
    ) -> Result<(Vec<f64>, Vec<SystemTime>), Error> {
        let mut op_times = vec![];
        let mut accumulated_times = vec![];
        match op {
            &Operation::Mkdir(ref file, ref _mode) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::make_dir(path) {
                    Ok(_) => {
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
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
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
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
                            op_times.push(begin.elapsed()?.as_secs_f64());
                            accumulated_times.push(SystemTime::now());
                        }
                        Err(_err) => {}
                    }
                } else {
                    match Fs::remove_file(path) {
                        Ok(_) => {
                            op_times.push(begin.elapsed()?.as_secs_f64());
                            accumulated_times.push(SystemTime::now());
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
                                op_times.push(begin.elapsed()?.as_secs_f64());
                                accumulated_times.push(SystemTime::now());
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
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
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
                            op_times.push(begin.elapsed()?.as_secs_f64());
                            accumulated_times.push(SystemTime::now());
                        }
                        Err(_err) => {}
                    }
                } else if path.is_dir() {
                    match Fs::open_dir(path) {
                        Ok(_) => {
                            op_times.push(begin.elapsed()?.as_secs_f64());
                            accumulated_times.push(SystemTime::now());
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
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Stat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Fstat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::Statx(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                match Fs::metadata(path) {
                    Ok(_) => {
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
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
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
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
                        op_times.push(begin.elapsed()?.as_secs_f64());
                        accumulated_times.push(SystemTime::now());
                    }
                    Err(_err) => {}
                }
            }
            &Operation::GetRandom(ref len) => {
                let begin = SystemTime::now();

                let mut rand_content = vec![0u8; *len];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_content);

                op_times.push(begin.elapsed()?.as_secs_f64());
                accumulated_times.push(SystemTime::now());
            }
            &Operation::Clone(pid) => {
                // get the list of operations done by the process which is going to be cloned
                match self.processes.iter().find(|p| p.pid() == pid) {
                    Some(process) => {
                        crossbeam::thread::scope(|s| {
                            s.spawn(|_| -> Result<(), Error> {
                                for (_op_id, op) in process.ops() {
                                    let (mut op_time, mut accumulated_time) =
                                        self.exec(op, base_path)?;
                                    op_times.append(&mut op_time);
                                    accumulated_times.append(&mut accumulated_time);
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

        Ok((op_times, accumulated_times))
    }
}
