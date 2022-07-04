use crate::error::Error;
use crate::fs::Fs;
use crate::micro::{micro_setup, print_output, random_leaf, BenchFn};
use crate::plotter::Plotter;
use crate::progress::Progress;
use crate::stats::Statistics;
use crate::{Bench, BenchResult, Config, ResultMode};
use async_channel::{unbounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use piston_window::event_id::{AFTER_RENDER, CLOSE};
use piston_window::{EventLoop, GenericEvent, PistonWindow, WindowSettings};
use plotters::prelude::{ChartBuilder, IntoDrawingArea, LineSeries, Palette, Palette99, WHITE};
use plotters_piston::draw_piston_window;
use rand::{thread_rng, Rng, RngCore};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

pub struct RealTimeBench {
    config: Config,
    sender: Sender<Signal>,
    receiver: Receiver<Signal>,
}

impl Bench for RealTimeBench {
    fn new(config: Config) -> Result<Self, Error> {
        let (sender, receiver) = unbounded();
        Ok(Self {
            config,
            sender,
            receiver,
        })
    }

    fn setup(&self, path: &PathBuf, invalidate_cache: bool) -> Result<(), Error> {
        micro_setup(
            self.config.file_size,
            self.config.fileset_size,
            path,
            invalidate_cache,
        )
    }

    fn run(&self, bench_fn: Option<BenchFn>) -> Result<(), Error> {
        let bench_fn = bench_fn.ok_or(Error::InvalidConfig(
            "A valid bench function not provided".to_string(),
        ))?;

        sudo::escalate_if_needed()?;

        // setup the paths before run
        let mut root_path = self.config.mount_paths[0].clone();
        root_path.push(bench_fn.to_string());
        let invalidate_cache = if bench_fn == BenchFn::ColdRead {
            true
        } else {
            false
        };
        self.setup(&root_path, invalidate_cache)?;

        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let receiver = self.receiver.clone();
        let ops = Arc::new(RwLock::new(0.0));
        let shared_ops = ops.clone();
        let shared_bench_fn = bench_fn.clone();
        let io_size = self.config.io_size;
        let file_set_size = self.config.fileset_size;
        let handle = std::thread::spawn(move || -> Result<(Vec<SystemTime>, u64), Error> {
            RealTimeBench::realtime_op(
                shared_bench_fn,
                io_size,
                file_set_size,
                &root_path,
                receiver,
                shared_ops,
            )
        });

        self.plot(ops, handle, progress_style, bench_fn.to_string())?;

        Ok(())
    }
}

enum Signal {
    Start,
    Stop,
}

impl RealTimeBench {
    fn plot(
        &self,
        ops: Arc<RwLock<f64>>,
        handle: JoinHandle<Result<(Vec<SystemTime>, u64), Error>>,
        style: ProgressStyle,
        bench_fn: String,
    ) -> Result<(), Error> {
        let fps = 20; // frame per second
        let length = 20; // plot length in second
        let n_data_points: usize = (fps * length) as usize;
        let tick_length = 1000 / fps; // length of each tick in millisecond
        let max_ticks = self.config.run_time as u64 * fps; // how many times the plot data is updated

        let mut window: PistonWindow =
            WindowSettings::new("Real Time Micro Benchmarks", [800, 500])
                .samples(4)
                .build()?;
        window.set_max_fps(fps);

        let mut data = VecDeque::from(vec![0f64; n_data_points + 1]);
        let mut ticks = 0;

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(bench_fn.clone());
        let progress = Progress::start(bar.clone());

        // capitalize the function name
        let capitalized_bench_fn = bench_fn
            .chars()
            .nth(0)
            .unwrap()
            .to_uppercase()
            .collect::<String>()
            + &bench_fn[1..];

        let mut max = 0.0;
        while let Some(event) = draw_piston_window(&mut window, |b| {
            if data.len() == n_data_points + 1 {
                data.pop_front();
            }
            let current_ops = *ops
                .read()
                .map_err(|err| Error::SyncError(err.to_string()))?;

            let current_ops = (current_ops * 1000.0) / tick_length as f64;
            data.push_back(current_ops);
            if current_ops > max {
                max = current_ops;
            }
            // reset the ops so far
            *ops.write()
                .map_err(|err| Error::SyncError(err.to_string()))? = 0f64;

            let root = b.into_drawing_area();
            root.fill(&WHITE)?;

            let mut cc = ChartBuilder::on(&root)
                .margin::<f64>(10.0)
                .caption::<String, (&str, f64)>(capitalized_bench_fn.clone(), ("sans-serif", 30.0))
                .x_label_area_size::<f64>(50.0)
                .y_label_area_size::<f64>(50.0)
                .build_cartesian_2d(0.0..n_data_points as f64, 0f64..max + (max * 0.1))?;

            cc.configure_mesh()
                .x_label_formatter(&|x| format!("{}", -(length as f64) + (*x as f64 / fps as f64)))
                .y_label_formatter(&|y| {
                    if *y >= 1000.0 {
                        format!("{:e}", y)
                    } else {
                        y.to_string()
                    }
                })
                .x_desc("Time (s)")
                .y_desc("Ops/s")
                .axis_desc_style::<(&str, f64)>(("sans-serif", 15.0))
                .draw()?;

            cc.draw_series(LineSeries::new(
                (0..).zip(data.iter()).map(|(a, b)| (a as f64, *b)),
                &Palette99::pick(0),
            ))?;

            ticks += 1;

            Ok(())
        }) {
            // if the plot window is rendered successfully, send the start signal to start benchmarking
            if event.event_id() == AFTER_RENDER && ticks == 1 {
                self.sender
                    .try_send(Signal::Start)
                    .map_err(|err| Error::SyncError(err.to_string()))?;
            }
            // if we have reached the max runtime or the plot window is closed, stop benchmarking
            if ticks >= max_ticks || event.event_id() == CLOSE {
                // plotting is finished
                let (behaviour, ops) = match self.sender.try_send(Signal::Stop) {
                    Ok(_) => {
                        bar.set_message("waiting for collected data...");
                        handle.join().unwrap()?
                    }
                    Err(e) => return Err(Error::SyncError(e.to_string())),
                };

                let run_time = Duration::from_millis((ticks * tick_length) as u64);

                bar.set_message("analysing data...");
                let ops_in_window = Statistics::ops_in_window(&behaviour, run_time)?;
                let ops_per_seconds = ops_in_window
                    .iter()
                    .map(|(_t, ops_s)| *ops_s as f64)
                    .collect::<Vec<_>>();
                let analysed_data = Statistics::new(&ops_per_seconds)?.analyse()?;

                let mut behaviour_records = vec![];
                for (time, ops_s) in ops_in_window.iter() {
                    behaviour_records.push([time.to_string(), ops_s.to_string()].to_vec().into());
                }

                progress.finish_with_message(&format!("{} finished", bench_fn))?;
                if bench_fn == "mkdir" || bench_fn == "mknod" {
                    print_output(
                        ops,
                        run_time.as_secs_f64(),
                        self.config.io_size,
                        &analysed_data,
                        false
                    );
                } else {
                    print_output(
                        ops,
                        run_time.as_secs_f64(),
                        self.config.io_size,
                        &analysed_data,
                        true
                    );
                }


                // log behaviour result
                let behaviour_header = ["time".to_string(), "ops".to_string()].to_vec();
                let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
                mkdir_behaviour_results.add_records(behaviour_records)?;
                let mut file_name = self.config.log_path.clone();
                file_name.push(format!("{}_{}.csv", self.config.fs_names[0], bench_fn));
                mkdir_behaviour_results.log(&file_name)?;
                let mut plotter_mkdir_behaviour = Plotter::new();
                plotter_mkdir_behaviour.add_coordinates(
                    &file_name,
                    None,
                    &ResultMode::Behaviour,
                )?;

                // plot the behaviour result
                let mut file_name = self.config.log_path.clone();
                file_name.push(format!("{}.svg", bench_fn));
                plotter_mkdir_behaviour.line_chart(
                    Some("Time (s)"),
                    Some("Ops/s"),
                    Some(&capitalized_bench_fn),
                    false,
                    false,
                    &file_name,
                )?;

                break;
            }
        }

        Ok(())
    }

    fn realtime_op(
        op: BenchFn,
        io_size: usize,
        fileset_size: usize,
        path: &PathBuf,
        receiver: Receiver<Signal>,
        ops: Arc<RwLock<f64>>,
    ) -> Result<(Vec<SystemTime>, u64), Error> {
        let mut behaviour = vec![];
        let mut idx = 0;

        // create a big vector filled with random content
        let mut rand_content = vec![0u8; 8192 * io_size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_content);
        let mut start = false;
        loop {
            match receiver.try_recv() {
                Ok(Signal::Stop) => {
                    return Ok((behaviour, idx));
                }
                Ok(Signal::Start) => {
                    start = true;
                }
                _ => {}
            }

            if start {
                match op {
                    BenchFn::Mkdir => {
                        // find a random leaf from the existing directory hierarchy and
                        // generate some (random number between 0 to 100) directories inside it
                        let start = SystemTime::now();
                        let random_dir = random_leaf(&path)?;
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
                                    *ops.write()? += 1.0;
                                }
                                Err(e) => {
                                    error!("error: {:?}", e);
                                }
                            }
                        }
                    }
                    BenchFn::Mknod => {
                        let mut file_name = path.clone();
                        file_name.push(idx.to_string());
                        match Fs::make_file(&file_name) {
                            Ok(_) => {
                                behaviour.push(SystemTime::now());
                                idx = idx + 1;
                                *ops.write()? += 1.0;
                            }
                            Err(e) => {
                                error!("error: {:?}", e);
                            }
                        }
                    }
                    BenchFn::Read | BenchFn::ColdRead => {
                        let file = thread_rng().gen_range(0..fileset_size);
                        let mut file_name = path.clone();
                        file_name.push(file.to_string());
                        let mut read_buffer = vec![0u8; io_size];
                        let mut file = Fs::open_file(&file_name)?;
                        match file.read_exact(&mut read_buffer) {
                            Ok(_) => {
                                behaviour.push(SystemTime::now());
                                idx += 1;
                                *ops.write()? += 1.0;
                            }
                            Err(e) => {
                                println!("error: {:?}", e);
                            }
                        }
                    }
                    BenchFn::Write => {
                        let rand_content_index =
                            thread_rng().gen_range(0..(8192 * io_size) - io_size - 1);
                        let mut content = rand_content
                            [rand_content_index..(rand_content_index + io_size)]
                            .to_vec();

                        let file = thread_rng().gen_range(1..fileset_size);
                        let mut file_name = path.clone();
                        file_name.push(file.to_string());
                        let mut file = Fs::open_file(&file_name)?;
                        match file.write_all(&mut content) {
                            Ok(_) => {
                                behaviour.push(SystemTime::now());
                                idx += 1;
                                *ops.write()? += 1.0;
                            }
                            Err(e) => {
                                println!("error: {:?}", e);
                            }
                        }
                    }
                    BenchFn::WriteSync => {
                        let rand_content_index =
                            thread_rng().gen_range(0..(8192 * io_size) - io_size - 1);
                        let mut content = rand_content
                            [rand_content_index..(rand_content_index + io_size)]
                            .to_vec();

                        let file = thread_rng().gen_range(1..fileset_size);
                        let mut file_name = path.clone();
                        file_name.push(file.to_string());
                        let mut file = Fs::open_file(&file_name)?;
                        match file.write_all(&mut content) {
                            Ok(_) => {
                                file.sync_data()?;
                                behaviour.push(SystemTime::now());
                                idx += 1;
                                *ops.write()? += 1.0;
                            }
                            Err(e) => {
                                println!("error: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}
