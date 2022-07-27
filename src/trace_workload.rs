use crate::error::Error;
use crate::format::{percent_format, time_format, time_format_by_unit, time_unit};
use crate::fs::Fs;
use crate::plotter::{Indexes, Plotter};
use crate::progress::Progress;
use crate::{Bench, BenchFn, BenchResult, Config, Record};
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use strace_parser::{FileType, Operation, OperationType, Parser, Process};

pub struct TraceWorkloadRunner {
    config: Config,
    processes: Vec<Arc<Mutex<Process>>>, // list of processes with their operations list
    files: Vec<FileType>,                 // the files and directories accessed and logged by trace
}

impl Bench for TraceWorkloadRunner {
    fn new(config: Config) -> Result<Self, Error> {
        // parse the trace log file and extract the operations
        let mut parser = Parser::new(config.workload.clone());

        let style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");
        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("parsing {}", Fs::path_to_str(&config.workload)?));
        let progress = Progress::start(bar.clone());

        let processes = parser.parse()?;
        let processes = processes
            .into_iter()
            .map(|process| Arc::new(Mutex::new(process)))
            .collect::<Vec<_>>();
        let files = parser.existing_files()?;
        let mut files = Vec::from_iter(files.into_iter());
        files.retain(|file_type| file_type.path() != "/" && file_type.path() != ".");

        progress.finish_and_clear()?;

        Ok(Self {
            config,
            processes,
            files,
        })
    }

    // create the directory hierarchy of the workload
    fn setup(&self, path: &PathBuf, _invalidate_cache: bool) -> Result<(), Error> {
        Fs::cleanup(path)?;

        let style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");
        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("setting up {}", Fs::path_to_str(path)?));
        let progress = Progress::start(bar.clone());

        for file_type in self.files.iter() {
            match file_type {
                FileType::File(file_path, size) => {
                    let new_path = Fs::map_path(path, file_path)?;
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
                FileType::Dir(dir_path, _) => {
                    let new_path = Fs::map_path(path, dir_path)?;
                    // create the directory
                    if !new_path.exists() {
                        Fs::make_dir_all(&new_path)?;
                    }
                },
                _ => {}
            }
        }

        progress.finish_and_clear()?;

        Ok(())
    }

    fn run(&self, _bench_fn: Option<BenchFn>) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let mount_paths = self.config.mount_paths.clone();
        let fs_names = self.config.fs_names.clone();

        // the output file to keep the stats, which are also printed to terminal
        let mut output_path = self.config.log_path.clone();
        output_path.push("output.txt");
        let output = OpenOptions::new()
            .write(true)
            .append(false)
            .create(true)
            .open(output_path)?;

        for (idx, mount_path) in mount_paths.iter().enumerate() {
            let mut base_path = mount_path.clone();
            base_path.push("trace_workload");
            base_path.push("files");

            let (op_times_records, accumulated_times_records, op_time_unit, accumulated_time_unit) =
                self.replay(&base_path, &fs_names[idx], progress_style.clone(), &output)?;

            let op_times_header = ["op".to_string(), format!("time ({})", op_time_unit)].to_vec();
            let accumulated_times_header = [
                "pid".to_string(),
                "op".to_string(),
                format!("accumulated_time ({})", accumulated_time_unit),
            ]
            .to_vec();

            // log the results
            let mut results = BenchResult::new(op_times_header.clone());
            results.add_records(op_times_records)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_op_times_trace_workload.csv",
                self.config.fs_names[idx]
            ));
            results.log(&file_name)?;

            let mut op_times_plotter = Plotter::new();
            op_times_plotter.add_coordinates(
                results.records,
                None,
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut accumulated_times_plotter = Plotter::new();
            let mut accumulated_times_results = BenchResult::new(accumulated_times_header.clone());
            for accumulated_times_records in accumulated_times_records.into_iter() {
                accumulated_times_results.add_records(accumulated_times_records.clone())?;

                accumulated_times_plotter.add_coordinates(
                    accumulated_times_records,
                    None,
                    Indexes::new(2, false, 1, None, None),
                )?;
            }

            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_accumulated_times.csv",
                self.config.fs_names[idx]
            ));
            accumulated_times_results.log(&file_name)?;

            // plot the results
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_op_times_trace_workload.svg",
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
                "{}_accumulated_times.svg",
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

impl TraceWorkloadRunner {
    // replay the processes' operations in parallel
    fn replay(
        &self,
        base_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
        output: &File,
    ) -> Result<(Vec<Record>, Vec<Vec<Record>>, String, String), Error> {
        self.setup(&base_path, false)?;

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
        let start_time = SystemTime::now();
        let mut handles = vec![];
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

                    accumulated_times
                        .push((execution_result.pid, execution_result.accumulated_times));

                    for (op_name, (time, num)) in execution_result.op_summaries.iter() {
                        if let Some((t, n)) = op_summaries.get_mut(op_name) {
                            *t += time;
                            *n += num;
                        } else {
                            op_summaries.insert(op_name.clone(), (*time, *num));
                        }

                        total_op_time += time;
                        total_ops += num;
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
        let (_pid, last) = accumulated_times[accumulated_times.len() - 1].clone();
        let accumulated_time_unit = time_unit(last[last.len() - 1]);
        let mut idx = 0;
        for (pid, accumulated_time) in accumulated_times.iter() {
            let mut accumulated_times_record = vec![];
            for system_time in accumulated_time.iter() {
                accumulated_times_record.push(
                    vec![
                        pid.to_string(),
                        (idx + 1).to_string(),
                        time_format_by_unit(*system_time, accumulated_time_unit)?.to_string(),
                    ]
                    .into(),
                );

                idx += 1;
            }
            accumulated_times_records.push(accumulated_times_record);
        }
        // for (idx, (pid, system_time)) in accumulated_times.iter().enumerate() {
        //     accumulated_times_records.push(
        //         vec![
        //             time_format_by_unit(*system_time, accumulated_time_unit)?.to_string(),
        //             (idx + 1).to_string(),
        //         ]
        //         .into(),
        //     );
        // }

        // output the stats to both terminal and a file
        let mut writer = BufWriter::new(output);
        writer.write(format!("{}\n", fs_name).as_ref())?;

        println!("{:25} {}\n", "replay time:", time_format(end));
        println!("{:25} {}", "total operations time:", time_format(total_op_time));
        println!("{:25} {}", "total operations: ", total_ops);
        println!("{:25} {}\n", "total processes: ", process_summaries.len());

        writer.write(format!("{:25} {}\n\n", "replay time:", time_format(end)).as_ref())?;
        writer.write(format!("{:25} {}\n", "total operations time:", time_format(total_op_time)).as_ref())?;
        writer.write(format!("{:25} {}\n", "total operations: ", total_ops).as_ref())?;
        writer.write(format!("{:25} {}\n\n", "total processes: ", process_summaries.len()).as_ref())?;


        for (pid, summary) in process_summaries {
            println!("{}", pid);
            writer.write(format!("{}\n", pid).as_ref())?;
            // sort the summaries by the time spend on each operation
            let mut summary = summary.into_iter().collect::<Vec<(String, (f64, u16))>>();
            summary.sort_by(|(_, (t1, _)), (_, (t2, _))| t2.partial_cmp(t1).unwrap());
            for (op, (time, num)) in summary.iter() {
                println!(
                    "{:7} {:12} {:12} ({:9} of total time)",
                    num,
                    op,
                    time_format(*time),
                    percent_format((time / total_op_time) * 100.0)
                );

                writer.write(
                    format!(
                        "{:7} {:12} {:12} ({:9} of total time)\n",
                        num,
                        op,
                        time_format(*time),
                        percent_format((time / total_op_time) * 100.0)
                    ).as_ref()
                )?;
            }
        }

        println!("\n---------------");
        writer.write(format!("\n---------------\n").as_ref())?;
        writer.flush()?;

        Ok((
            op_times_records,
            accumulated_times_records,
            op_time_unit.to_string(),
            accumulated_time_unit.to_string(),
        ))
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
        //      value: a pair of (time spend for this operation so far, number of this operation)
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
        // mark the operation as executed
        self.executed();

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

        Ok((op_time, system_time))
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    #[test]
    fn many_spawns() {
        let start = SystemTime::now();
        let threads = 1500;
        let mut handles = vec![];
        for thread in 0..threads {
            handles.push(std::thread::spawn(move || -> usize {
                std::thread::sleep(Duration::from_secs(1));
                1
            }));
        }

        let mut finished = 0;
        for handle in handles {
            finished += handle.join().unwrap();
        }
        let end = start.elapsed().unwrap().as_secs_f64();
        println!("finished threads: {}", finished);
        println!("time: {}", end);
    }
}
