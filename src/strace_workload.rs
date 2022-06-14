use crate::error::Error;
use crate::format::{percent_format, time_format, time_format_by_unit, time_unit};
use crate::fs::Fs;
use crate::plotter::Plotter;
use crate::progress::Progress;
use crate::{Bench, BenchFn, BenchResult, Config, Record, ResultMode};
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use strace_parser::{FileDir, Operation, OperationType, Parser, Process};

pub struct StraceWorkloadRunner {
    config: Config,
    processes: Vec<Arc<Mutex<Process>>>, // list of processes with their operations list
    files: Vec<FileDir>,                 // the files and directories accessed and logged by strace
}

impl Bench for StraceWorkloadRunner {
    fn new(config: Config) -> Result<Self, Error> {
        // parse the strace log file and extract the operations
        let mut parser = Parser::new(config.workload.clone());
        let processes = parser.parse()?;
        let processes = processes
            .into_iter()
            .map(|process| Arc::new(Mutex::new(process)))
            .collect::<Vec<_>>();
        let files = parser.existing_files()?;
        let mut files = Vec::from_iter(files.into_iter());

        files.retain(|file_dir| file_dir.path() != "/");

        Ok(Self {
            config,
            processes,
            files,
        })
    }

    fn run(&self, _bench_fn: Option<BenchFn>) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let mount_paths = self.config.mount_paths.clone();
        let fs_names = self.config.fs_names.clone();

        for (idx, mount_path) in mount_paths.iter().enumerate() {
            let mut base_path = mount_path.clone();
            base_path.push("strace_workload");

            let (op_times_records, accumulated_times_records, op_time_unit, accumulated_time_unit) =
                self.replay(&base_path, &fs_names[idx], progress_style.clone())?;

            let op_times_header = ["op".to_string(), format!("time ({})", op_time_unit)].to_vec();
            let accumulated_times_header = [
                format!("time ({})", accumulated_time_unit),
                "ops".to_string(),
            ]
            .to_vec();

            // log the results
            let mut results = BenchResult::new(op_times_header.clone());
            results.add_records(op_times_records)?;
            let mut file_name_p = self.config.log_path.clone();
            file_name_p.push(format!(
                "{}_op_times_strace_workload.csv",
                self.config.fs_names[idx]
            ));
            results.log(&file_name_p)?;

            let mut op_times_plotter = Plotter::new();
            op_times_plotter.add_coordinates(
                &file_name_p,
                None,
                &ResultMode::OpTimes,
            )?;

            let mut accumulated_times_results = BenchResult::new(accumulated_times_header.clone());
            accumulated_times_results.add_records(accumulated_times_records)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times_workload.csv",
                self.config.fs_names[idx]
            ));
            accumulated_times_results.log(&file_name)?;

            let mut accumulated_times_plotter = Plotter::new();
            accumulated_times_plotter.add_coordinates(
                &file_name,
                None,
                &ResultMode::Behaviour,
            )?;

            // plot the results
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_op_times_strace_workload.svg",
                self.config.fs_names[idx]
            ));
            op_times_plotter.line_chart(
                Some("Operations"),
                Some(&format!("Time ({})", op_time_unit)),
                Some(&format!(
                    "Operation times from replayed logs ({})",
                    self.config.fs_names[idx]
                )),
                false,
                false,
                &file_name,
            )?;

            // plot the accumulated results
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times_workload.svg",
                self.config.fs_names[idx]
            ));
            accumulated_times_plotter.line_chart(
                Some(&format!("Time ({})", accumulated_time_unit)),
                Some("Operations"),
                Some(&format!(
                    "Accumulated times from replayed logs ({})",
                    self.config.fs_names[idx]
                )),
                false,
                false,
                &file_name,
            )?;
        }

        println!(
            "results logged to: {}",
            Fs::path_to_str(&self.config.log_path)?
        );

        Ok(())
    }
}

impl StraceWorkloadRunner {
    // replay the processes' operations in parallel
    fn replay(
        &self,
        base_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<(Vec<Record>, Vec<Record>, String, String), Error> {
        self.setup(&base_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("replaying logs ({})", fs_name));
        let progress = Progress::start(bar);

        let start = SystemTime::now();
        let mut op_times = vec![];
        let mut accumulated_times = vec![];
        let mut op_summaries: HashMap<String, (f64, u16)> = HashMap::new();
        let mut process_summaries = vec![];

        // replay the processes' operations in parallel
        let mut handles = vec![];
        let start_time = SystemTime::now();
        for process in self.processes.iter() {
            let base_path = base_path.clone();
            let process = process.clone();
            let handle = std::thread::spawn(move || -> Result<ExecutionResult, Error> {
                process.lock()?.run(&base_path, start_time)
            });

            handles.push(handle);
        }

        let mut total_op_time = 0f64;
        let mut total_ops = 0;
        for handle in handles {
            match handle.join() {
                Ok(execution_result) => {
                    let mut execution_result = execution_result?;
                    op_times.append(&mut execution_result.op_times);

                    accumulated_times.append(&mut execution_result.accumulated_times);
                    // let last = if accumulated_times.is_empty() {
                    //     0f64
                    // } else { accumulated_times[accumulated_times.len() - 1]};
                    // for accumulated_time in execution_result.accumulated_times {
                    //     accumulated_times.push(last + accumulated_time);
                    // }

                    for (ck, (ct, cn)) in execution_result.op_summaries.iter() {
                        if let Some((t, n)) = op_summaries.get_mut(ck) {
                            *t += ct;
                            *n += cn;
                        } else {
                            op_summaries.insert(ck.clone(), (*ct, *cn));
                        }

                        total_op_time += ct;
                        total_ops += cn;
                    }

                    process_summaries.push((execution_result.pid, execution_result.op_summaries));
                }
                Err(_err) => {}
            }
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        let mut op_times_records = vec![];
        let op_time_unit = time_unit(op_times[0]);
        for (idx, time) in op_times.iter().enumerate() {
            op_times_records.push(
                vec![
                    idx.to_string(),
                    time_format_by_unit(*time, op_time_unit)?.to_string(),
                ]
                .into(),
            );
        }

        let mut accumulated_times_records = vec![];
        // let first = accumulated_times[0];
        let last = accumulated_times[accumulated_times.len() - 1];
        let accumulated_time_unit = time_unit(last);
        accumulated_times_records.push(vec!["0".to_string(), "0".to_string()].into());
        for (idx, system_time) in accumulated_times.iter().enumerate() {
            accumulated_times_records.push(
                vec![
                    time_format_by_unit(*system_time, accumulated_time_unit)?.to_string(),
                    (idx + 1).to_string(),
                ]
                .into(),
            );
        }

        println!("{:20} {}\n", "total run time:", time_format(end));

        println!("{:20} {}", "total time:", time_format(total_op_time));
        println!("{:20} {}", "total operations: ", total_ops);
        println!("{:20} {}\n", "total processes: ", process_summaries.len());

        for (pid, summary) in process_summaries {
            println!("{}", pid);
            // sort the summaries by the time spend on each operation
            let mut summary = summary.into_iter().collect::<Vec<(String, (f64, u16))>>();
            summary.sort_by(|(_, (t1, _)), (_, (t2, _))| t2.partial_cmp(t1).unwrap());
            for (op, (time, num)) in summary.iter() {
                println!(
                    "{:7} {:12} {:12} ({:5} of total time)",
                    num,
                    op,
                    time_format(*time),
                    percent_format((time / total_op_time) * 100.0)
                );
            }
        }

        println!("\n---------------");
        Ok((
            op_times_records,
            accumulated_times_records,
            op_time_unit.to_string(),
            accumulated_time_unit.to_string(),
        ))
    }

    // create the directory hierarchy of the workload
    pub fn setup(&self, base_path: &PathBuf) -> Result<(), Error> {
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
}

struct ExecutionResult {
    pid: usize,
    op_times: Vec<f64>,
    accumulated_times: Vec<f64>,
    op_summaries: HashMap<String, (f64, u16)>,
}

trait Runner {
    fn run(
        &mut self,
        base_path: &PathBuf,
        start_time: SystemTime,
    ) -> Result<ExecutionResult, Error>;
}

impl Runner for Process {
    fn run(
        &mut self,
        base_path: &PathBuf,
        start_time: SystemTime,
    ) -> Result<ExecutionResult, Error> {
        let mut op_times = vec![];
        let mut accumulated_times = vec![];
        // summary of operations:
        //      key: operation name
        //      value: (time spend for this operation so far, number of this operation)
        let mut op_summaries: HashMap<String, (f64, u16)> = HashMap::new();

        let mut ops = self.ops().clone();
        while !ops.is_empty() {
            // remove and take the first operation from the list
            let shared_op = ops.remove(0);
            match shared_op.op().lock() {
                Ok(mut op) => {
                    if op.can_be_executed()? {
                        match op.execute(base_path, start_time) {
                            Ok((op_time, system_time)) => {
                                op_times.push(op_time);
                                accumulated_times.push(system_time);
                                if let Some((t, n)) = op_summaries.get_mut(&op.name()) {
                                    *t += op_time;
                                    *n += 1;
                                } else {
                                    op_summaries.insert(op.name(), (op_time, 1));
                                }
                            }
                            Err(_err) => {}
                        }
                    } else {
                        // add the removed operation to the end of the list to be executed later
                        ops.push(shared_op.clone());
                    }
                }
                Err(_err) => {}
            }
        }

        Ok(ExecutionResult {
            pid: self.pid(),
            op_times,
            accumulated_times,
            op_summaries,
        })
    }
}

trait Executer {
    fn execute(&mut self, base_path: &PathBuf, start_time: SystemTime)
        -> Result<(f64, f64), Error>;
}

impl Executer for Operation {
    fn execute(
        &mut self,
        base_path: &PathBuf,
        start_time: SystemTime,
    ) -> Result<(f64, f64), Error> {
        let (op_time, system_time) = match self.op_type() {
            &OperationType::Mkdir(ref file, ref _mode) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::make_dir(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Mknod(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                // create a file and sets its size and offset
                let begin = SystemTime::now();
                let file = Fs::make_file(path)?;
                file.set_len(0)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Remove(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let path = PathBuf::from(path);

                let begin = SystemTime::now();
                if path.is_dir() {
                    Fs::remove_dir(&path)?;
                    let now = SystemTime::now();
                    let end = now.duration_since(begin)?.as_secs_f64();
                    let system_time = now.duration_since(start_time)?.as_secs_f64();
                    (end, system_time)
                } else {
                    Fs::remove_file(&path)?;
                    let now = SystemTime::now();
                    let end = now.duration_since(begin)?.as_secs_f64();
                    let system_time = now.duration_since(start_time)?.as_secs_f64();
                    (end, system_time)
                }
            }
            OperationType::Read(ref file, ref offset, ref len) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let mut buffer = vec![0u8; *len];

                let mut file = Fs::open_file(path)?;
                let begin = SystemTime::now();
                Fs::read_at(&mut file, &mut buffer, *offset as u64)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Write(ref file, ref offset, ref len, ref _content) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let mut rand_content = vec![0u8; *len];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_content);

                let mut file = Fs::open_file(path)?;

                let begin = SystemTime::now();
                Fs::write_at(&mut file, &mut rand_content, *offset as u64)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::OpenAt(ref file, ref _offset) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                if path.is_file() {
                    Fs::open_file(path)?;
                    let now = SystemTime::now();
                    let end = now.duration_since(begin)?.as_secs_f64();
                    let system_time = now.duration_since(start_time)?.as_secs_f64();
                    (end, system_time)
                } else {
                    Fs::open_dir(path)?;
                    let now = SystemTime::now();
                    let end = now.duration_since(begin)?.as_secs_f64();
                    let system_time = now.duration_since(start_time)?.as_secs_f64();
                    (end, system_time)
                }
            }
            &OperationType::Truncate(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::truncate(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Stat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::metadata(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Fstat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::metadata(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Statx(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::metadata(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::StatFS(ref file) => {
                let _path = Fs::map_path(base_path, file.path()?)?;
                return Err(Error::NoTimeRecord(self.name()));
                // TODO there is no statfs in std::fs and I may need to implement it in another way?
            }
            &OperationType::Fstatat(ref file) => {
                let path = Fs::map_path(base_path, file.path()?)?;
                let begin = SystemTime::now();
                Fs::metadata(path)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::Rename(ref file, ref to) => {
                let from = Fs::map_path(base_path, file.path()?)?;
                let to = Fs::map_path(base_path, to)?;

                let begin = SystemTime::now();
                Fs::rename(&from, &to)?;
                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            &OperationType::GetRandom(ref len) => {
                let begin = SystemTime::now();

                let mut rand_content = vec![0u8; *len];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_content);

                let now = SystemTime::now();
                let end = now.duration_since(begin)?.as_secs_f64();
                let system_time = now.duration_since(start_time)?.as_secs_f64();
                (end, system_time)
            }
            _ => {
                return Err(Error::NoTimeRecord(self.name()));
            }
        };

        // mark the operation as executed
        self.executed();
        Ok((op_time, system_time))
    }
}
