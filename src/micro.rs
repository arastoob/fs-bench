use crate::format::{percent_format, time_format, time_format_by_unit, time_unit};
use crate::plotter::Plotter;
use crate::sample::{AnalysedData, Sample};
use crate::timer::Timer;
use crate::{BenchResult, Error, Fs, Progress, Record, ResultMode};
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use rand::{thread_rng, Rng, RngCore};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct MicroBench {
    io_size: usize,
    run_time: f64,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
}

impl MicroBench {
    pub fn new(
        io_size: String,
        run_time: f64,
        mount_path: PathBuf,
        fs_name: String,
        log_path: PathBuf,
    ) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            io_size,
            run_time,
            mount_path,
            fs_name,
            log_path,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        let rt = Duration::from_secs(self.run_time as u64); // running time
        self.behaviour_bench(rt)?;
        let max_rt = Duration::from_secs(60 * 5);
        self.throughput_bench(max_rt)?;

        println!("results logged to: {}", Fs::path_to_str(&self.log_path)?);

        Ok(())
    }

    fn behaviour_bench(&self, run_time: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let (mkdir_ops_s, mkdir_behaviour, mkdir_times, mkdir_time_uint) =
            self.mkdir(run_time, progress_style.clone())?;
        let (mknod_ops_s, mknod_behaviour, mknod_times, mknod_time_uint) =
            self.mknod(run_time, progress_style.clone())?;
        let (read_ops_s, read_behaviour, read_times, read_time_uint) =
            self.read(run_time, progress_style.clone())?;
        let (write_ops_s, write_behaviour, write_times, write_time_uint) =
            self.write(run_time, progress_style)?;
        //
        let ops_s_header = [
            "operation".to_string(),
            "runtime(s)".to_string(),
            "ops/s".to_string(),
            "ops/s_lb".to_string(),
            "ops/s_ub".to_string(),
        ]
        .to_vec();

        // ops/s logs and plots
        let mut ops_s_results = BenchResult::new(ops_s_header);
        ops_s_results.add_record(mkdir_ops_s)?;
        ops_s_results.add_record(mknod_ops_s)?;
        ops_s_results.add_record(read_ops_s)?;
        ops_s_results.add_record(write_ops_s)?;

        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_ops_per_second.csv", self.fs_name));
        ops_s_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpsPerSecond)?;
        file_name.set_extension("svg");
        plotter.bar_chart(Some("Operation"), Some("Ops/s"), None, &file_name)?;

        // sample iteration average times logs and plots
        let times_header = ["op".to_string(), format!("time ({})", mkdir_time_uint)].to_vec();
        let mut mkdir_times_results = BenchResult::new(times_header);
        mkdir_times_results.add_records(mkdir_times)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mkdir_iteration_times.csv", self.fs_name));
        mkdir_times_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpTimes)?;
        file_name.set_extension("svg");
        plotter.point_series(
            Some("Sample iteration"),
            Some(format!("Average time ({})", mkdir_time_uint).as_str()),
            None,
            &file_name,
        )?;

        let times_header = ["op".to_string(), format!("time ({})", mknod_time_uint)].to_vec();
        let mut mknod_times_results = BenchResult::new(times_header);
        mknod_times_results.add_records(mknod_times)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mknod_iteration_times.csv", self.fs_name));
        mknod_times_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpTimes)?;
        file_name.set_extension("svg");
        plotter.point_series(
            Some("Sample iteration"),
            Some(format!("Average time ({})", mknod_time_uint).as_str()),
            None,
            &file_name,
        )?;

        let times_header = ["op".to_string(), format!("time ({})", read_time_uint)].to_vec();
        let mut read_times_results = BenchResult::new(times_header);
        read_times_results.add_records(read_times)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_read_iteration_times.csv", self.fs_name));
        read_times_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpTimes)?;
        file_name.set_extension("svg");
        plotter.point_series(
            Some("Sample iteration"),
            Some(format!("Average time ({})", read_time_uint).as_str()),
            None,
            &file_name,
        )?;

        let times_header = ["op".to_string(), format!("time ({})", write_time_uint)].to_vec();
        let mut write_times_results = BenchResult::new(times_header);
        write_times_results.add_records(write_times)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_write_iteration_times.csv", self.fs_name));
        write_times_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpTimes)?;
        file_name.set_extension("svg");
        plotter.point_series(
            Some("Sample iteration"),
            Some(format!("Average time ({})", write_time_uint).as_str()),
            None,
            &file_name,
        )?;

        // behaviour plots and files
        let behaviour_header = ["second".to_string(), "ops".to_string()].to_vec();

        let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
        mkdir_behaviour_results.add_records(mkdir_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mkdir.csv", self.fs_name));
        mkdir_behaviour_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
        mknod_behaviour_results.add_records(mknod_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mknod.csv", self.fs_name));
        mknod_behaviour_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
        read_behaviour_results.add_records(read_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_read.csv", self.fs_name));
        read_behaviour_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        let mut write_behaviour_results = BenchResult::new(behaviour_header);
        write_behaviour_results.add_records(write_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_write.csv", self.fs_name));
        write_behaviour_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        Ok(())
    }

    fn throughput_bench(&self, max_rt: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let throughput_header = ["file_size".to_string(), "throughput".to_string()].to_vec();

        let read_throughput = self.read_throughput(max_rt, progress_style.clone())?;
        let write_throughput = self.write_throughput(max_rt, progress_style)?;

        let mut read_throughput_results = BenchResult::new(throughput_header.clone());
        read_throughput_results.add_records(read_throughput)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_read_throughput.csv", self.fs_name));
        read_throughput_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Throughput)?;
        file_name.set_extension("svg");
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
            &file_name,
        )?;

        let mut write_throughput_results = BenchResult::new(throughput_header);
        write_throughput_results.add_records(write_throughput)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_write_throughput.csv", self.fs_name));
        write_throughput_results.log(&file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Throughput)?;
        file_name.set_extension("svg");
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
            &file_name,
        )?;

        Ok(())
    }

    fn mkdir(
        &self,
        run_time: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>, Vec<Record>, &str), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mkdir");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message("mkdir");
        let progress = Progress::start(bar.clone());

        // creating the root directory to generate the benchmark directories inside it
        Fs::make_dir(&root_path)?;

        let (sender, receiver) = channel();
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, u64), Error> {
                let mut times = vec![];
                let mut behaviour = vec![];
                let mut idx = 0;
                loop {
                    match receiver.try_recv() {
                        Ok(true) => {
                            return Ok((times, behaviour, idx));
                        }
                        _ => {
                            let mut dir_name = root_path.clone();
                            dir_name.push(idx.to_string());
                            let begin = SystemTime::now();
                            match Fs::make_dir(&dir_name) {
                                Ok(()) => {
                                    let end = begin.elapsed()?.as_secs_f64();
                                    times.push(end);
                                    behaviour.push(SystemTime::now());
                                    idx = idx + 1;
                                }
                                Err(e) => {
                                    error!("error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            });

        std::thread::sleep(run_time);
        let (times, behaviour, idx) = match sender.send(true) {
            Ok(_) => {
                bar.set_message("waiting for collected data...");
                handle.join().unwrap()?
            }
            Err(e) => return Err(Error::SyncError(e.to_string())),
        };

        bar.set_message("analysing data...");
        let analysed_data = Sample::new(&times)?.analyse()?;

        progress.finish_with_message("mkdir finished")?;
        self.print_micro(idx, run_time.as_secs_f64(), &analysed_data);

        let ops_per_second_record = Record {
            fields: [
                "mkdir".to_string(),
                run_time.as_secs_f64().to_string(),
                analysed_data.ops_per_second.to_string(),
                analysed_data.ops_per_second_lb.to_string(),
                analysed_data.ops_per_second_ub.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour, run_time)?;

        let time_unit = time_unit(analysed_data.mean_lb);
        let mut time_records = vec![];
        for (idx, time) in analysed_data.sample_means.iter().enumerate() {
            time_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, time_unit)?.to_string(),
                ]
                .to_vec(),
            })
        }

        Ok((
            ops_per_second_record,
            behaviour_records,
            time_records,
            time_unit,
        ))
    }

    fn mknod(
        &self,
        run_time: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>, Vec<Record>, &str), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mknod");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message("mknod");
        let progress = Progress::start(bar.clone());

        // creating the root directory to generate the benchmark files inside it
        Fs::make_dir(&root_path)?;

        let (sender, receiver) = channel();
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, u64), Error> {
                let mut times = vec![];
                let mut behaviour = vec![];
                let mut idx = 0;
                loop {
                    match receiver.try_recv() {
                        Ok(true) => {
                            return Ok((times, behaviour, idx));
                        }
                        _ => {
                            let mut file_name = root_path.clone();
                            file_name.push(idx.to_string());
                            let begin = SystemTime::now();
                            match Fs::make_file(&file_name) {
                                Ok(_) => {
                                    let end = begin.elapsed()?.as_secs_f64();
                                    times.push(end);
                                    behaviour.push(SystemTime::now());
                                    idx = idx + 1;
                                }
                                Err(e) => {
                                    error!("error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            });

        std::thread::sleep(run_time);
        let (times, behaviour, idx) = match sender.send(true) {
            Ok(_) => {
                bar.set_message("waiting for collected data...");
                handle.join().unwrap()?
            }
            Err(e) => return Err(Error::SyncError(e.to_string())),
        };

        bar.set_message("analysing data...");
        let analysed_data = Sample::new(&times)?.analyse()?;

        progress.finish_with_message("mknod finished")?;
        self.print_micro(idx, run_time.as_secs_f64(), &analysed_data);

        let ops_per_second_record = Record {
            fields: [
                "mknod".to_string(),
                run_time.as_secs_f64().to_string(),
                analysed_data.ops_per_second.to_string(),
                analysed_data.ops_per_second_lb.to_string(),
                analysed_data.ops_per_second_ub.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour, run_time)?;

        let time_unit = time_unit(analysed_data.mean_lb);
        let mut time_records = vec![];
        for (idx, time) in analysed_data.sample_means.iter().enumerate() {
            time_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, time_unit)?.to_string(),
                ]
                .to_vec(),
            })
        }

        Ok((
            ops_per_second_record,
            behaviour_records,
            time_records,
            time_unit,
        ))
    }

    fn read(
        &self,
        run_time: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>, Vec<Record>, &str), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message("read");
        let progress = Progress::start(bar.clone());

        // creating the root directory to generate the benchmark files inside it
        Fs::make_dir(&root_path)?;

        let size = self.io_size;
        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let mut file = Fs::make_file(&file_name)?;

            // generate a buffer of size io size filled with random data
            let mut rand_buffer = vec![0u8; size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_buffer);

            file.write(&rand_buffer)?;
        }

        let (sender, receiver) = channel();
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, u64), Error> {
                let mut times = vec![];
                let mut behaviour = vec![];
                let mut idx = 0;
                let mut read_buffer = vec![0u8; size];
                loop {
                    match receiver.try_recv() {
                        Ok(true) => {
                            return Ok((times, behaviour, idx));
                        }
                        _ => {
                            let file = thread_rng().gen_range(1..1001);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let begin = SystemTime::now();
                            match Fs::open_read(&file_name, &mut read_buffer) {
                                Ok(_) => {
                                    let end = begin.elapsed()?.as_secs_f64();
                                    times.push(end);
                                    behaviour.push(SystemTime::now());
                                    idx += 1;
                                }
                                Err(e) => {
                                    println!("error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            });

        std::thread::sleep(run_time);
        let (times, behaviour, idx) = match sender.send(true) {
            Ok(_) => {
                bar.set_message("waiting for collected data...");
                handle.join().unwrap()?
            }
            Err(e) => return Err(Error::SyncError(e.to_string())),
        };

        bar.set_message("analysing data...");
        let analysed_data = Sample::new(&times)?.analyse()?;

        progress.finish_with_message("read finished")?;
        self.print_micro(idx, run_time.as_secs_f64(), &analysed_data);

        let ops_per_second_record = Record {
            fields: [
                "read".to_string(),
                run_time.as_secs_f64().to_string(),
                analysed_data.ops_per_second.to_string(),
                analysed_data.ops_per_second_lb.to_string(),
                analysed_data.ops_per_second_ub.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour, run_time)?;

        let time_unit = time_unit(analysed_data.mean_lb);
        let mut time_records = vec![];
        for (idx, time) in analysed_data.sample_means.iter().enumerate() {
            time_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, time_unit)?.to_string(),
                ]
                .to_vec(),
            })
        }

        Ok((
            ops_per_second_record,
            behaviour_records,
            time_records,
            time_unit,
        ))
    }

    fn write(
        &self,
        run_time: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>, Vec<Record>, &str), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("write");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message("write");
        let progress = Progress::start(bar.clone());

        // creating the root directory to generate the benchmark files inside it
        Fs::make_dir(&root_path)?;

        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            Fs::make_file(&file_name)?;
        }

        // create a big vector filled with random content
        let size = self.io_size;
        let mut rand_content = vec![0u8; 8192 * size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_content);

        let (sender, receiver) = channel();
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, u64), Error> {
                let mut times = vec![];
                let mut behaviour = vec![];
                let mut idx = 0;
                loop {
                    match receiver.try_recv() {
                        Ok(true) => {
                            return Ok((times, behaviour, idx));
                        }
                        _ => {
                            let rand_content_index =
                                thread_rng().gen_range(0..(8192 * size) - size - 1);
                            let mut content = rand_content
                                [rand_content_index..(rand_content_index + size)]
                                .to_vec();

                            let file = thread_rng().gen_range(1..1001);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let begin = SystemTime::now();
                            match Fs::open_write(&file_name, &mut content) {
                                Ok(_) => {
                                    let end = begin.elapsed()?.as_secs_f64();
                                    times.push(end);
                                    behaviour.push(SystemTime::now());
                                    idx += 1;
                                }
                                Err(e) => {
                                    println!("error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            });

        std::thread::sleep(run_time);
        let (times, behaviour, idx) = match sender.send(true) {
            Ok(_) => {
                bar.set_message("waiting for collected data...");
                handle.join().unwrap()?
            }
            Err(e) => return Err(Error::SyncError(e.to_string())),
        };

        bar.set_message("analysing data...");
        let analysed_data = Sample::new(&times)?.analyse()?;

        progress.finish_with_message("write finished")?;
        self.print_micro(idx, run_time.as_secs_f64(), &analysed_data);

        let ops_per_second_record = Record {
            fields: [
                "write".to_string(),
                run_time.as_secs_f64().to_string(),
                analysed_data.ops_per_second.to_string(),
                analysed_data.ops_per_second_lb.to_string(),
                analysed_data.ops_per_second_ub.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour, run_time)?;

        let time_unit = time_unit(analysed_data.mean_lb);
        let mut time_records = vec![];
        for (idx, time) in analysed_data.sample_means.iter().enumerate() {
            time_records.push(Record {
                fields: [
                    idx.to_string(),
                    time_format_by_unit(*time, time_unit)?.to_string(),
                ]
                .to_vec(),
            })
        }

        Ok((
            ops_per_second_record,
            behaviour_records,
            time_records,
            time_unit,
        ))
    }

    fn read_throughput(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read_throughput"));
        let progress = Progress::start(bar);

        // creating the root directory to generate the test files inside it
        Fs::make_dir(&root_path)?;

        // create a big file filled with random content
        let mut file_name = root_path.clone();
        file_name.push("big_file".to_string());
        let mut file = Fs::make_file(&file_name)?;

        let file_size = 1000 * 1000 * 100 * 2; // 200 MB
        let mut rand_buffer = vec![0u8; file_size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_buffer);
        file.write(&rand_buffer)?;

        let mut read_size = 1000;
        let mut throughputs = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;

        let start = SystemTime::now();
        // read 1000, 10000, 100000, 1000000, 10000000, 100000000 sizes from the big file
        while read_size <= file_size / 2 {
            let mut read_buffer = vec![0u8; read_size];
            // process read 10 times and then log the mean of the 10 runs
            let mut times = vec![];
            for _ in 0..10 {
                let rand_index = thread_rng().gen_range(0..file_size - read_size - 1) as u64;
                let begin = SystemTime::now();
                // random read from a random index
                match Fs::open_read_at(&file_name, &mut read_buffer, rand_index) {
                    Ok(_) => {
                        let end = begin.elapsed()?.as_secs_f64();
                        times.push(end);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }

            let sample = Sample::new(&times)?;
            let mean = sample.mean();
            let throughput = read_size as f64 / mean; // B/s
            throughputs.push((read_size, throughput));
            read_size *= 10;

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            progress.finish()?;
        } else {
            progress.abandon_with_message("read exceeded the max runtime")?;
        }

        println!("{:11} {}", "run time:", time_format(end));

        let mut throughput_records = vec![];
        for (size, throughput) in throughputs {
            let size = Byte::from_bytes(size as u128);
            let adjusted_size = size.get_appropriate_unit(false);

            let throughput = Byte::from_bytes(throughput as u128);
            let adjusted_throughput = throughput.get_appropriate_unit(false);
            println!(
                "[{:10} {}/s]",
                adjusted_size.format(0),
                adjusted_throughput.format(3)
            );

            throughput_records.push(Record {
                fields: [size.to_string(), throughput.to_string()].to_vec(),
            });
        }

        println!();
        Ok(throughput_records)
    }

    fn write_throughput(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("write");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("{:5}", "write_throughput"));
        let progress = Progress::start(bar);

        // creating the root directory to generate the test files inside it
        Fs::make_dir(&root_path)?;

        // create a file to write into
        let mut file_name = root_path.clone();
        file_name.push("big_file".to_string());
        Fs::make_file(&file_name)?;

        let buffer_size = 1000 * 1000 * 100 * 2; // 200 MB
        let mut rand_content = vec![0u8; buffer_size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_content);

        let mut write_size = 1000;
        let mut throughputs = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;

        let start = SystemTime::now();
        // write 1000, 10000, 100000, 1000000, 10000000, 100000000 sizes to the big file
        while write_size <= buffer_size / 2 {
            // process write 10 times and then log the mean of the 10 runs
            let mut times = vec![];
            for _ in 0..10 {
                let rand_content_index = thread_rng().gen_range(0..buffer_size - write_size - 1);
                let mut content =
                    rand_content[rand_content_index..(rand_content_index + write_size)].to_vec();

                let begin = SystemTime::now();
                // random read from a random index
                match Fs::open_write(&file_name, &mut content) {
                    Ok(_) => {
                        let end = begin.elapsed()?.as_secs_f64();
                        times.push(end);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }

            let sample = Sample::new(&times)?;
            let mean = sample.mean();
            let throughput = write_size as f64 / mean; // B/s
            throughputs.push((write_size, throughput));
            write_size *= 10;

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            progress.finish()?;
        } else {
            progress.abandon_with_message("write exceeded the max runtime")?;
        }

        println!("{:11} {}", "run time:", time_format(end));

        let mut throughput_records = vec![];
        for (size, throughput) in throughputs {
            let size = Byte::from_bytes(size as u128);
            let adjusted_size = size.get_appropriate_unit(false);

            let throughput = Byte::from_bytes(throughput as u128);
            let adjusted_throughput = throughput.get_appropriate_unit(false);
            println!(
                "[{:10} {}/s]",
                adjusted_size.format(0),
                adjusted_throughput.format(3)
            );

            throughput_records.push(Record {
                fields: [size.to_string(), throughput.to_string()].to_vec(),
            });
        }

        println!();
        Ok(throughput_records)
    }

    fn print_micro(&self, iterations: u64, run_time: f64, analysed_data: &AnalysedData) {
        println!("{:18} {}", "iterations:", iterations);
        println!("{:18} {}", "run time:", time_format(run_time));
        println!(
            "{:18} [{}, {}]",
            "op time (95% CI):",
            time_format(analysed_data.mean_lb),
            time_format(analysed_data.mean_ub),
        );
        println!(
            "{:18} [{}, {}]",
            "ops/s (95% CI):", analysed_data.ops_per_second_lb, analysed_data.ops_per_second_ub
        );
        println!(
            "{:18} {}",
            "outliers:",
            percent_format(analysed_data.outliers_percentage)
        );
        println!();
    }
}
