use crate::error::Error;
use crate::fs::Fs;
use crate::micro::{micro_setup, print_output, random_leaf};
use crate::plotter::{Indexes, Plotter};
use crate::progress::Progress;
use crate::stats::Statistics;
use crate::BenchFn::Mknod;
use crate::{Bench, BenchFn, BenchResult, Config, Record};
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use rand::{thread_rng, Rng, RngCore};
use std::io::{Read, Write};
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

    fn setup(&self, path: &PathBuf, invalidate_cache: bool) -> Result<(), Error> {
        micro_setup(
            self.config.file_size,
            self.config.fileset_size,
            path,
            invalidate_cache,
        )
    }

    fn run(&self, _bench_fn: Option<BenchFn>) -> Result<(), Error> {
        sudo::escalate_if_needed()?;

        let rt = Duration::from_secs(self.config.run_time as u64); // running time
        self.behaviour_bench(rt)?;

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
        let mut plotter_cold_read_behaviour = Plotter::new();
        let mut plotter_write_behaviour = Plotter::new();
        let mut plotter_write_sync_behaviour = Plotter::new();
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
            let (cold_read_ops_s, cold_read_behaviour, cold_read_times) = self.micro_op(
                BenchFn::ColdRead,
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
            let (write_sync_ops_s, write_sync_behaviour, write_sync_times) = self.micro_op(
                BenchFn::WriteSync,
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
            ops_s_results.add_record(cold_read_ops_s)?;
            ops_s_results.add_record(write_ops_s)?;
            ops_s_results.add_record(write_sync_ops_s)?;

            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_ops_per_second.csv", self.config.fs_names[idx]));
            ops_s_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(
                ops_s_results.records,
                None,
                Indexes::new(0, true, 2, Some(3), Some(4)),
            )?;
            file_name.set_extension("svg");
            plotter.bar_chart(
                Some("Operation"),
                Some("Ops/s"),
                Some(&format!("Ops/s ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            // log behaviour results
            let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
            mkdir_behaviour_results.add_records(mkdir_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_mkdir.csv", self.config.fs_names[idx]));
            mkdir_behaviour_results.log(&file_name)?;
            plotter_mkdir_behaviour.add_coordinates(
                mkdir_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
            mknod_behaviour_results.add_records(mknod_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_mknod.csv", self.config.fs_names[idx]));
            mknod_behaviour_results.log(&file_name)?;
            plotter_mknod_behaviour.add_coordinates(
                mknod_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
            read_behaviour_results.add_records(read_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_read.csv", self.config.fs_names[idx]));
            read_behaviour_results.log(&file_name)?;
            plotter_read_behaviour.add_coordinates(
                read_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut cold_read_behaviour_results = BenchResult::new(behaviour_header.clone());
            cold_read_behaviour_results.add_records(cold_read_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_cold_read.csv", self.config.fs_names[idx]));
            cold_read_behaviour_results.log(&file_name)?;
            plotter_cold_read_behaviour.add_coordinates(
                cold_read_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut write_behaviour_results = BenchResult::new(behaviour_header.clone());
            write_behaviour_results.add_records(write_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_write.csv", self.config.fs_names[idx]));
            write_behaviour_results.log(&file_name)?;
            plotter_write_behaviour.add_coordinates(
                write_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            let mut write_sync_behaviour_results = BenchResult::new(behaviour_header.clone());
            write_sync_behaviour_results.add_records(write_sync_behaviour.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_write_sync.csv", self.config.fs_names[idx]));
            write_sync_behaviour_results.log(&file_name)?;
            plotter_write_sync_behaviour.add_coordinates(
                write_sync_behaviour,
                Some(self.config.fs_names[idx].clone()),
                Indexes::new(0, false, 1, None, None),
            )?;

            // log and plot sample iteration average ops/s
            let ops_s_samples_header = ["iterations".to_string(), "ops/s".to_string()].to_vec();
            let mut mkdir_times_results = BenchResult::new(ops_s_samples_header.clone());
            mkdir_times_results.add_records(mkdir_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_mkdir_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            mkdir_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(mkdir_times, None, Indexes::new(0, false, 1, None, None))?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Mkdir ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut mknod_times_results = BenchResult::new(ops_s_samples_header.clone());
            mknod_times_results.add_records(mknod_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_mknod_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            mknod_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(mknod_times, None, Indexes::new(0, false, 1, None, None))?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Mknod ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut read_times_results = BenchResult::new(ops_s_samples_header.clone());
            read_times_results.add_records(read_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_read_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            read_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(read_times, None, Indexes::new(0, false, 1, None, None))?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Read ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut cold_read_times_results = BenchResult::new(ops_s_samples_header.clone());
            cold_read_times_results.add_records(cold_read_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_cold_read_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            cold_read_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(
                cold_read_times,
                None,
                Indexes::new(0, false, 1, None, None),
            )?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Cold read ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut write_times_results = BenchResult::new(ops_s_samples_header.clone());
            write_times_results.add_records(write_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_write_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            write_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(write_times, None, Indexes::new(0, false, 1, None, None))?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Write ({})", self.config.fs_names[idx])),
                &file_name,
            )?;

            let mut write_sync_times_results = BenchResult::new(ops_s_samples_header.clone());
            write_sync_times_results.add_records(write_sync_times.clone())?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_write_sync_ops_s_period.csv",
                self.config.fs_names[idx]
            ));
            write_sync_times_results.log(&file_name)?;

            let mut plotter = Plotter::new();
            plotter.add_coordinates(
                write_sync_times,
                None,
                Indexes::new(0, false, 1, None, None),
            )?;
            file_name.set_extension("svg");
            plotter.point_series(
                Some("Sampling iterations"),
                Some("Average Ops/s"),
                Some(&format!("Write_sync ({})", self.config.fs_names[idx])),
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
        file_name.push("cold_read.svg");
        plotter_cold_read_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Cold read"),
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

        let mut file_name = self.config.log_path.clone();
        file_name.push("write_sync.svg");
        plotter_write_sync_behaviour.line_chart(
            Some("Time (s)"),
            Some("Ops/s"),
            Some("Write (full sync)"),
            false,
            false,
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

        let invalidate_cache = if op == BenchFn::ColdRead { true } else { false };
        self.setup(&root_path, invalidate_cache)?;

        let io_size = self.config.io_size;
        let fileset_size = self.config.fileset_size;
        let operation = op.clone();

        // create a big vector filled with random content
        let mut rand_content = vec![0u8; 8192 * io_size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_content);

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("{} ({})", op.to_string(), fs_name));
        let progress = Progress::start(bar.clone());

        let (sender, receiver) = channel();
        let handle = std::thread::spawn(move || -> Result<(Vec<SystemTime>, u64), Error> {
            let mut behaviour = vec![];
            let mut idx = 0;

            loop {
                match receiver.try_recv() {
                    Ok(true) => {
                        return Ok((behaviour, idx));
                    }
                    _ => match operation {
                        BenchFn::Mkdir => {
                            // find a random leaf from the existing directory hierarchy and
                            // generate some (random number between 0 to 100) directories inside it
                            let start = SystemTime::now();
                            let random_dir = random_leaf(&root_path)?;
                            let dirs = thread_rng().gen_range(0..100);
                            let end = start.elapsed()?;

                            for dir in 0..dirs {
                                let mut dir_name = random_dir.clone();
                                dir_name.push(dir.to_string());
                                match Fs::make_dir(&dir_name) {
                                    Ok(()) => {
                                        let now = SystemTime::now();
                                        // subtract the time for choosing a leaf randomly from the op time
                                        behaviour.push(now.checked_sub(end).unwrap_or(now));
                                        idx = idx + 1;
                                    }
                                    Err(e) => {
                                        error!("error: {:?}", e);
                                    }
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
                        BenchFn::Read | BenchFn::ColdRead => {
                            let file = thread_rng().gen_range(0..fileset_size);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let mut file = Fs::open_file(&file_name)?;
                            let mut read_buffer = vec![0u8; io_size];
                            match file.read_exact(&mut read_buffer) {
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
                            let rand_content_index = thread_rng().gen_range(0..8192 - io_size - 1);
                            let mut content = rand_content
                                [rand_content_index..(rand_content_index + io_size)]
                                .to_vec();

                            let file = thread_rng().gen_range(0..fileset_size);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let mut file = Fs::open_file(&file_name)?;
                            match file.write_all(&mut content) {
                                Ok(_) => {
                                    behaviour.push(SystemTime::now());
                                    idx += 1;
                                }
                                Err(e) => {
                                    println!("error: {:?}", e);
                                }
                            }
                        }
                        BenchFn::WriteSync => {
                            let rand_content_index = thread_rng().gen_range(0..8192 - io_size - 1);
                            let mut content = rand_content
                                [rand_content_index..(rand_content_index + io_size)]
                                .to_vec();

                            let file = thread_rng().gen_range(0..fileset_size);
                            let mut file_name = root_path.clone();
                            file_name.push(file.to_string());
                            let mut file = Fs::open_file(&file_name)?;
                            match file.write_all(&mut content) {
                                Ok(_) => {
                                    file.sync_data()?;
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
        if op == BenchFn::Mkdir || op == Mknod {
            print_output(idx, run_time.as_secs_f64(), io_size, &analysed_data, false);
        } else {
            print_output(idx, run_time.as_secs_f64(), io_size, &analysed_data, true);
        }

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
}
