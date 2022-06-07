use crate::error::Error;
use crate::fs::Fs;
use crate::micro::print_output;
use crate::plotter::Plotter;
use crate::progress::Progress;
use crate::sample::Sample;
use crate::{Bench, BenchResult, Config, Record, ResultMode};
use async_channel::{unbounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use piston_window::event_id::CLOSE;
use piston_window::{EventLoop, GenericEvent, PistonWindow, WindowSettings};
use plotters::prelude::{ChartBuilder, IntoDrawingArea, LineSeries, Palette, Palette99, WHITE};
use plotters_piston::draw_piston_window;
use rand::{thread_rng, Rng, RngCore};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

///
/// Benchmark function that is being run in real-time
///
#[derive(Debug, Clone)]
pub enum BenchFn {
    Mkdir,
    Mknod,
    Read,
    Write,
}

impl FromStr for BenchFn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mkdir" => Ok(BenchFn::Mkdir),
            "mknod" => Ok(BenchFn::Mknod),
            "read" => Ok(BenchFn::Read),
            "write" => Ok(BenchFn::Write),
            _ => Err("valid benckmark functions are: mkdir, mknod, read, write".to_string()),
        }
    }
}

impl Display for BenchFn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchFn::Mkdir => write!(f, "mkdir"),
            BenchFn::Mknod => write!(f, "mknod"),
            BenchFn::Read => write!(f, "read"),
            BenchFn::Write => write!(f, "write"),
        }
    }
}

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

    fn run(&self, bench_fn: Option<BenchFn>) -> Result<(), Error> {
        let bench_fn = bench_fn.ok_or(Error::InvalidConfig(
            "A valid bench function not provided".to_string(),
        ))?;

        let mut root_path = self.config.mount_paths[0].clone();
        root_path.push(bench_fn.to_string());
        Fs::cleanup(&root_path)?;

        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let mp = self.config.mount_paths[0].clone();
        let receiver = self.receiver.clone();
        let ops = Arc::new(RwLock::new(0.0));
        let shared_ops = ops.clone();
        let shared_bench_fn = bench_fn.clone();
        let io_size = self.config.io_size;
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, i32), Error> {
                match shared_bench_fn {
                    BenchFn::Mkdir => RealTimeBench::mkdir(receiver, mp, shared_ops),
                    BenchFn::Mknod => RealTimeBench::mknod(receiver, mp, shared_ops),
                    BenchFn::Read => RealTimeBench::read(io_size, receiver, mp, shared_ops),
                    _ => {
                        return Err(Error::Unknown("Not implemented".to_string()));
                    }
                }
            });

        self.plot(ops, handle, progress_style, bench_fn.to_string())?;

        Ok(())
    }
}

enum Signal {
    Stop,
}

impl RealTimeBench {
    fn plot(
        &self,
        ops: Arc<RwLock<f32>>,
        handle: JoinHandle<Result<(Vec<f64>, Vec<SystemTime>, i32), Error>>,
        style: ProgressStyle,
        bench_fn: String,
    ) -> Result<(), Error> {
        let fps: u32 = 20; // frame per second
        let length: u32 = 20; // plot length in second
        let n_data_points: usize = (fps * length) as usize;
        let tick_length = 1000 / fps; // length of each tick in millisecond
        let max_ticks = self.config.run_time as u32 * fps; // how many times the plot data is updated

        let mut window: PistonWindow = WindowSettings::new("Real Time CPU Usage", [800, 500])
            .samples(4)
            .build()?;
        window.set_max_fps(fps as u64);

        let mut data = VecDeque::from(vec![0f32; n_data_points + 1]);
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

            let current_ops = (current_ops * 1000.0) / tick_length as f32;
            data.push_back(current_ops);
            if current_ops > max {
                max = current_ops;
            }
            // reset the ops so far
            *ops.write()
                .map_err(|err| Error::SyncError(err.to_string()))? = 0.0;

            let root = b.into_drawing_area();
            root.fill(&WHITE)?;

            let mut cc = ChartBuilder::on(&root)
                .margin(10)
                .caption(capitalized_bench_fn.clone(), ("sans-serif", 30))
                .x_label_area_size(50)
                .y_label_area_size(50)
                .build_cartesian_2d(0..n_data_points as u32, 0f32..(max + 5000.0) as f32)?;

            cc.configure_mesh()
                .x_label_formatter(&|x| format!("{}", -(length as f32) + (*x as f32 / fps as f32)))
                .y_label_formatter(&|y| {
                    if *y >= 1000.0 {
                        format!("{:e}", y)
                    } else {
                        y.to_string()
                    }
                })
                .x_desc("Time (s)")
                .y_desc("Ops/s")
                .axis_desc_style(("sans-serif", 15))
                .draw()?;

            cc.draw_series(LineSeries::new(
                (0..).zip(data.iter()).map(|(a, b)| (a, *b)),
                &Palette99::pick(0),
            ))?;
            // .label(bench_fn.clone())
            // .legend(move |(x, y)| {
            //     Rectangle::new([(x - 5, y - 5), (x + 5, y + 5)], &Palette99::pick(0))
            // });

            // cc.configure_series_labels()
            //     .background_style(&WHITE.mix(0.8))
            //     .border_style(&BLACK)
            //     .draw()?;

            ticks += 1;

            Ok(())
        }) {
            if ticks >= max_ticks || event.event_id() == CLOSE {
                // plotting is finished
                let (times, behaviour, ops) = match self.sender.try_send(Signal::Stop) {
                    Ok(_) => {
                        bar.set_message("waiting for collected data...");
                        handle.join().unwrap()?
                    }
                    Err(e) => return Err(Error::SyncError(e.to_string())),
                };

                let run_time = Duration::from_millis((ticks * tick_length) as u64);

                bar.set_message("analysing data...");
                let analysed_data = Sample::new(&times)?.analyse()?;

                let behaviour_records = Record::ops_in_window(&behaviour, run_time)?;

                progress.finish_with_message(&format!("{} finished", bench_fn))?;
                print_output(ops as u64, run_time.as_secs_f64(), &analysed_data);

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

    fn mkdir(
        receiver: Receiver<Signal>,
        mount_path: PathBuf,
        ops: Arc<RwLock<f32>>,
    ) -> Result<(Vec<f64>, Vec<SystemTime>, i32), Error> {
        let mut root_path = mount_path.clone();
        root_path.push("mkdir");

        // creating the root directory to generate the benchmark directories inside it
        Fs::make_dir(&root_path)?;

        let mut times = vec![];
        let mut behaviour = vec![];
        let mut idx = 0;
        loop {
            match receiver.try_recv() {
                Ok(Signal::Stop) => {
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
                            *ops.write()? += 1.0;
                        }
                        Err(e) => {
                            error!("error: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    fn mknod(
        receiver: Receiver<Signal>,
        mount_path: PathBuf,
        ops: Arc<RwLock<f32>>,
    ) -> Result<(Vec<f64>, Vec<SystemTime>, i32), Error> {
        let mut root_path = mount_path.clone();
        root_path.push("mknod");

        // creating the root directory to generate the benchmark directories inside it
        Fs::make_dir(&root_path)?;

        let mut times = vec![];
        let mut behaviour = vec![];
        let mut idx = 0;
        loop {
            match receiver.try_recv() {
                Ok(Signal::Stop) => {
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
                            *ops.write()? += 1.0;
                        }
                        Err(e) => {
                            error!("error: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    fn read(
        io_size: usize,
        receiver: Receiver<Signal>,
        mount_path: PathBuf,
        ops: Arc<RwLock<f32>>,
    ) -> Result<(Vec<f64>, Vec<SystemTime>, i32), Error> {
        let mut root_path = mount_path.clone();
        root_path.push("read");

        // creating the root directory to generate the benchmark directories inside it
        Fs::make_dir(&root_path)?;

        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let mut file = Fs::make_file(&file_name)?;

            // generate a buffer of size io size filled with random data
            let mut rand_buffer = vec![0u8; io_size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_buffer);

            file.write(&rand_buffer)?;
        }

        let mut times = vec![];
        let mut behaviour = vec![];
        let mut idx = 0;
        let mut read_buffer = vec![0u8; io_size];
        loop {
            match receiver.try_recv() {
                Ok(Signal::Stop) => {
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
