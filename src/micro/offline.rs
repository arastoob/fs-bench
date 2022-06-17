use crate::error::Error;
use crate::format::time_format;
use crate::fs::Fs;
use crate::micro::{micro_setup, print_output};
use crate::plotter::Plotter;
use crate::progress::Progress;
use crate::stats::Statistics;
use crate::timer::Timer;
use crate::{Bench, BenchFn, BenchResult, Config, Record, ResultMode};
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use rand::{thread_rng, Rng, RngCore};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time::{Duration, SystemTime};

pub struct OfflineBench {
    config: Config,
}

impl Bench for OfflineBench {
    fn new(config: Config) -> Result<Self, Error> {
        Ok(Self { config })
    }

    fn setup(&self, path: &PathBuf) -> Result<(), Error> {
        micro_setup(self.config.io_size, self.config.fileset_size, path)
    }

    fn run(&self, _bench_fn: Option<BenchFn>) -> Result<(), Error> {
        let rt = Duration::from_secs(self.config.run_time as u64); // running time
        self.behaviour_bench(rt)?;
        let max_rt = Duration::from_secs(60 * 5);
        self.throughput_bench(max_rt)?;

        println!(
            "results logged to: {}",
            Fs::path_to_str(&self.config.log_path)?
        );

        Ok(())
    }
}

impl OfflineBench {
    fn behaviour_bench(&self, run_time: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let mut plotter_mkdir_behaviour = Plotter::new();
        let mut plotter_mknod_behaviour = Plotter::new();
        let mut plotter_read_behaviour = Plotter::new();
        let mut plotter_write_behaviour = Plotter::new();
        let behaviour_header = ["time".to_string(), "ops".to_string()].to_vec();

        for (idx, mount_path) in self.config.mount_paths.iter().enumerate() {
            let (mkdir_ops_s, mkdir_behaviour, mkdir_times) = self.micro_op(
                BenchFn::Mkdir,
                run_time,
                mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;
            let (mknod_ops_s, mknod_behaviour, mknod_times) = self.micro_op(
                BenchFn::Mknod,
                run_time,
                mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;
            let (read_ops_s, read_behaviour, read_times) = self.micro_op(
                BenchFn::Read,
                run_time,
                mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;
            let (write_ops_s, write_behaviour, write_times) = self.micro_op(
                BenchFn::Write,
                run_time,
                mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;

            let ops_s_header = [
                "operation".to_string(),
                "runtime(s)".to_string(),
                "ops/s".to_string(),
                "ops/s_lb".to_string(),
                "ops/s_ub".to_string(),
            ]
            .to_vec();

            // log and plot ops/s
            let mut ops_s_results = BenchResult::new(ops_s_header);
            ops_s_results.add_record(mkdir_ops_s)?;
            ops_s_results.add_record(mknod_ops_s)?;
            ops_s_results.add_record(read_ops_s)?;
            ops_s_results.add_record(write_ops_s)?;

            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_ops_per_second.csv", self.config.fs_names[idx]));
            ops_s_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(&file_name, None, &ResultMode::OpsPerSecond)?;
            file_name.set_extension("svg");
            plotter.bar_chart(
                Some("Operation"),
                Some("Ops/s"),
                Some(&format!("Ops/s ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            // log behaviour results
            let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
            mkdir_behaviour_results.add_records(mkdir_behaviour)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_mkdir.csv", self.config.fs_names[idx]));
            mkdir_behaviour_results.log(&file_name)?;
            plotter_mkdir_behaviour.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Behaviour,
            )?;

            let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
            mknod_behaviour_results.add_records(mknod_behaviour)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_mknod.csv", self.config.fs_names[idx]));
            mknod_behaviour_results.log(&file_name)?;
            plotter_mknod_behaviour.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Behaviour,
            )?;

            let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
            read_behaviour_results.add_records(read_behaviour)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_read.csv", self.config.fs_names[idx]));
            read_behaviour_results.log(&file_name)?;
            plotter_read_behaviour.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Behaviour,
            )?;

            let mut write_behaviour_results = BenchResult::new(behaviour_header.clone());
            write_behaviour_results.add_records(write_behaviour)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_write.csv", self.config.fs_names[idx]));
            write_behaviour_results.log(&file_name)?;
            plotter_write_behaviour.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Behaviour,
            )?;

            // log and plot sample iteration average ops/s
            let ops_s_samples_header = ["iterations".to_string(), "ops/s".to_string()].to_vec();
            let mut mkdir_times_results = BenchResult::new(ops_s_samples_header.clone());
            mkdir_times_results.add_records(mkdir_times)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_mkdir_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            mkdir_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(&file_name, None, &ResultMode::SampleOpsPerSecond)?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Mkdir ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut mknod_times_results = BenchResult::new(ops_s_samples_header.clone());
            mknod_times_results.add_records(mknod_times)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_mknod_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            mknod_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(&file_name, None, &ResultMode::SampleOpsPerSecond)?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Mknod ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut read_times_results = BenchResult::new(ops_s_samples_header.clone());
            read_times_results.add_records(read_times)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_read_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            read_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(&file_name, None, &ResultMode::SampleOpsPerSecond)?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Read ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut write_times_results = BenchResult::new(ops_s_samples_header.clone());
            write_times_results.add_records(write_times)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_write_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            write_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(&file_name, None, &ResultMode::SampleOpsPerSecond)?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Write ({})", self.config.fs_names[idx])),
                &file_name,
            )?;
        }

        // plot the behaviour results
        let mut file_name = self.config.log_path.clone();
        file_name.push("mkdir.svg");
        plotter_mkdir_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Mkdir"),
            false,
            false,
            &file_name,
        )?;

        let mut file_name = self.config.log_path.clone();
        file_name.push("mknod.svg");
        plotter_mknod_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Mknod"),
            false,
            false,
            &file_name,
        )?;

        let mut file_name = self.config.log_path.clone();
        file_name.push("read.svg");
        plotter_read_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Read"),
            false,
            false,
            &file_name,
        )?;

        let mut file_name = self.config.log_path.clone();
        file_name.push("write.svg");
        plotter_write_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Write"),
            false,
            false,
            &file_name,
        )?;

        Ok(())
    }

    fn throughput_bench(&self, max_rt: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let throughput_header = ["file_size".to_string(), "throughput".to_string()].to_vec();

        let mut read_plotter = Plotter::new();
        let mut write_plotter = Plotter::new();
        for (idx, mount_path) in self.config.mount_paths.iter().enumerate() {
            let read_throughput = self.read_throughput(
                max_rt,
                &mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;
            let write_throughput = self.write_throughput(
                max_rt,
                &mount_path,
                &self.config.fs_names[idx],
                progress_style.clone(),
            )?;

            let mut read_throughput_results = BenchResult::new(throughput_header.clone());
            read_throughput_results.add_records(read_throughput)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_read_throughput.csv", self.config.fs_names[idx]));
            read_throughput_results.log(&file_name)?;

            read_plotter.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Throughput,
            )?;

            let mut write_throughput_results = BenchResult::new(throughput_header.clone());
            write_throughput_results.add_records(write_throughput)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_write_throughput.csv",
                self.config.fs_names[idx]
            ));
            write_throughput_results.log(&file_name)?;

            write_plotter.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Throughput,
            )?;
        }

        let mut file_name = self.config.log_path.clone();
        file_name.push("read_throughput.svg");
        read_plotter.line_chart(
            Some("File size (B)"),
            Some("Throughput (B/s)"),
            Some("Read Throughput"),
            true,
            true,
            &file_name,
        )?;

        let mut file_name = self.config.log_path.clone();
        file_name.push("write_throughput.svg");
        write_plotter.line_chart(
            Some("File size (B)"),
            Some("Throughput (B/s)"),
            Some("Write Throughput"),
            true,
            true,
            &file_name,
        )?;

        Ok(())
    }

    fn micro_op(
        &self,
        op: BenchFn,
        run_time: Duration,
        mount_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>, Vec<Record>), Error> {
        let mut root_path = mount_path.clone();
        root_path.push(op.to_string());
        self.setup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("{} ({})", op.to_string(), fs_name));
        let progress = Progress::start(bar.clone());

        let size = self.config.io_size;
        let fileset_size = self.config.fileset_size;
        let operation = op.clone();
        let (sender, receiver) = channel();
        let handle = std::thread::spawn(move || -> Result<(Vec<SystemTime>, u64), Error> {
            let mut behaviour = vec![];
            let mut idx = 0;

            // create a big vector filled with random content
            let mut rand_content = vec![0u8; 8192 * size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_content);

            loop {
                match receiver.try_recv() {
                    Ok(true) => {
                        return Ok((behaviour, idx));
                    }
                    _ => match operation {
                        BenchFn::Mkdir => {
                            let mut dir_name = root_path.clone();
                            dir_name.push(idx.to_string());
                            match Fs::make_dir(&dir_name) {
                                Ok(()) => {
                                    behaviour.push(SystemTime::now());
                                    idx = idx + 1;
                                }
                                Err(e) => {
                                    error!("error: {:?}", e);
                                }
                            }
                        }
                        BenchFn::Mknod => {
                            let mut file_name = root_path.clone();
                            file_name.push(idx.to_string());
                            match Fs::make_file(&file_name) {
                                Ok(_) => {
                                    behaviour.push(SystemTime::now());
                                    idx = idx + 1;
                                }
                                Err(e) => {
                                    error!("error: {:?}", e);
                                }
                            }
                        }
                        BenchFn::Read => {
                            let file = thread_rng().gen_range(0..fileset_size);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());

                            let mut file = Fs::open_file(&file_name)?;
                            let mut read_buffer = vec![0u8; size];
                            match Fs::read(&mut file, &mut read_buffer) {
                                Ok(_) => {
                                    behaviour.push(SystemTime::now());
                                    idx += 1;
                                }
                                Err(e) => {
                                    println!("error: {:?}", e);
                                }
                            }
                        }
                        BenchFn::Write => {
                            let rand_content_index =
                                thread_rng().gen_range(0..(8192 * size) - size - 1);
                            let mut content = rand_content
                                [rand_content_index..(rand_content_index + size)]
                                .to_vec();

                            let file = thread_rng().gen_range(0..fileset_size);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let mut file = Fs::open_file(&file_name)?;
                            match Fs::write(&mut file, &mut content) {
                                Ok(_) => {
                                    behaviour.push(SystemTime::now());
                                    idx += 1;
                                }
                                Err(e) => {
                                    println!("error: {:?}", e);
                                }
                            }
                        }
                    },
                }
            }
        });

        std::thread::sleep(run_time);
        let (behaviour, idx) = match sender.send(true) {
            Ok(_) => {
                bar.set_message(format!(
                    "{} ({}): waiting for collected data...",
                    op.to_string(),
                    fs_name
                ));
                handle.join().unwrap()?
            }
            Err(e) => return Err(Error::SyncError(e.to_string())),
        };

        bar.set_message(format!(
            "{} ({}): analysing data...",
            op.to_string(),
            fs_name
        ));
        let ops_in_window = Statistics::ops_in_window(&behaviour, run_time)?;
        let ops_per_seconds = ops_in_window
            .iter()
            .map(|(_t, ops_s)| *ops_s as f64)
            .collect::<Vec<_>>();
        let analysed_data = Statistics::new(&ops_per_seconds)?.analyse()?;

        progress.finish_with_message(&format!("{} ({}) finished", op.to_string(), fs_name))?;
        print_output(idx, run_time.as_secs_f64(), &analysed_data);

        let mut behaviour_records = vec![];
        for (time, ops_s) in ops_in_window.iter() {
            behaviour_records.push([time.to_string(), ops_s.to_string()].to_vec().into());
        }

        let ops_per_second_record = Record {
            fields: [
                op.to_string(),
                run_time.as_secs_f64().to_string(),
                analysed_data.mean.to_string(),
                analysed_data.mean_lb.to_string(),
                analysed_data.mean_ub.to_string(),
            ]
            .to_vec(),
        };

        let mut ops_s_samples_records = vec![];
        for (idx, ops_s) in analysed_data.sample_means.iter().enumerate() {
            ops_s_samples_records.push([idx.to_string(), ops_s.to_string()].to_vec().into());
        }

        Ok((
            ops_per_second_record,
            behaviour_records,
            ops_s_samples_records,
        ))
    }

    fn read_throughput(
        &self,
        max_rt: Duration,
        mount_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {
        let mut root_path = mount_path.clone();
        root_path.push("read");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("read_throughput ({})", fs_name));
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

            let sample = Statistics::new(&times)?;
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
            progress.abandon_with_message(&format!(
                "read_throughput ({}) exceeded the max runtime",
                fs_name
            ))?;
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

            throughput_records.push(vec![size.to_string(), throughput.to_string()].into());
        }

        println!();
        Ok(throughput_records)
    }

    fn write_throughput(
        &self,
        max_rt: Duration,
        mount_path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {
        let mut root_path = mount_path.clone();
        root_path.push("write");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("write_throughput ({})", fs_name));
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

            let sample = Statistics::new(&times)?;
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
            progress.abandon_with_message(&format!(
                "write_throughput ({}) exceeded the max runtime",
                fs_name
            ))?;
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

            throughput_records.push(vec![size.to_string(), throughput.to_string()].into());
        }

        println!();
        Ok(throughput_records)
    }
}
