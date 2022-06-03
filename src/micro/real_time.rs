use std::collections::VecDeque;
use std::path::PathBuf;
use std::str::FromStr;
use async_channel::{Receiver, Sender, unbounded};
use std::time::{Duration, SystemTime};
use indicatif::ProgressStyle;
use log::error;
use piston_window::{EventLoop, PistonWindow, WindowSettings};
use plotters::prelude::{ChartBuilder, Color, IntoDrawingArea, LineSeries, Palette, Palette99, Rectangle, WHITE, BLACK};
use plotters_piston::draw_piston_window;
use rand::Rng;
use crate::{Bench, Config, Record};
use crate::error::Error;
use crate::fs::Fs;
use crate::sample::Sample;

///
/// Benchmark function that is being run in real-time
///
#[derive(Debug)]
pub enum BenchFn {
    Mkdir,
    Mknod,
    Read,
    Write
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

pub struct RealTimeBench {
    config: Config,
    sender: Sender<Signal>,
    receiver: Receiver<Signal>
}

impl Bench for RealTimeBench {
    fn new(config: Config) -> Result<Self, Error> {
        let (sender, receiver) = unbounded();
        Ok(Self {
            config,
            sender,
            receiver
        })
    }


    fn run(&self, bench_fn: Option<BenchFn>) -> Result<(), Error> {
        if bench_fn.is_none() {
            return Err(Error::InvalidConfig("A valid bench function not provided".to_string()));
        }

        let rt = Duration::from_secs(self.config.run_time as u64);
        let mp = self.config.mount_paths[0].clone();
        let fsn = self.config.fs_names[0].clone();
        let sender = self.sender.clone();
        let receiver = self.receiver.clone();
        std::thread::spawn(move || {
            RealTimeBench::mkdir(sender, receiver, rt, mp, fsn).unwrap();
        });

        self.plot();

        Ok(())
    }
}

enum Signal {
    Stop,
    Fetch(Sender<f64>),
    Continue
}

impl RealTimeBench {

    fn plot(&self) {

        let fps: u32 = 5;
        let length: u32 = self.config.run_time as u32; // benchmark run time
        let n_data_points: usize = (fps * length) as usize;
        let max_ticks = length * fps; // how many times the plot data is updated

        let (fetch_sender, fetch_receiver) = unbounded();

        let mut window: PistonWindow  = WindowSettings::new("Real Time CPU Usage", [450, 300])
            .samples(4)
            .build()
            .unwrap();
        window.set_max_fps(fps as u64);

        let mut data = VecDeque::from(vec![0f32; n_data_points + 1]);
        let mut ticks = 0;

        while let Some(_event) = draw_piston_window(&mut window, |b| {

            if data.len() == n_data_points + 1 {
                data.pop_front();
            }
            data.push_back(self.fetch_data(fetch_sender.clone(), fetch_receiver.clone())?);


            let root = b.into_drawing_area();
            root.fill(&WHITE)?;


            let mut cc = ChartBuilder::on(&root)
                .margin(10)
                .caption("Operation per second", ("sans-serif", 30))
                .x_label_area_size(40)
                .y_label_area_size(50)
                .build_cartesian_2d(0..n_data_points as u32, 0f32..20000f32)?;

            cc.configure_mesh()
                .x_label_formatter(&|x| format!("{}", -(length as f32) + (*x as f32 / fps as f32)))
                .y_label_formatter(&|y| format!("{}", *y  as u32))
                .x_labels(15)
                .y_labels(5)
                .x_desc("Seconds")
                .y_desc("Ops/s")
                .axis_desc_style(("sans-serif", 15))
                .draw()?;

            cc.draw_series(LineSeries::new(
                (0..).zip(data.iter()).map(|(a, b)| (a, *b)),
                &Palette99::pick(1),
            ))?
                .label(format!("ext4 {}", 1))
                .legend(move |(x, y)| {
                    Rectangle::new([(x - 5, y - 5), (x + 5, y + 5)], &Palette99::pick(1))
                });

            cc.configure_series_labels()
                .background_style(&WHITE.mix(0.8))
                .border_style(&BLACK)
                .draw()?;

            ticks += 1;


            Ok(())
        }) {
            if ticks >= max_ticks {
                println!("finished");
                break;
            }
            // if event.event_id() == CLOSE {
            //     println!("event: {:?}", event);
            // }
        }
    }

    fn fetch_data(&self, fetch_sender: Sender<f64>, fetch_receiver: Receiver<f64>) -> Result<f32, Error> {

        self.sender.try_send(Signal::Fetch(fetch_sender)).unwrap();

        loop {
            match fetch_receiver.try_recv() {
                Ok(data) => {
                    return Ok(data as f32);
                },
                _ => {}
            }
        }

    }

    fn mkdir(
        sender: Sender<Signal>,
        receiver: Receiver<Signal>,
        run_time: Duration,
        mount_path: PathBuf,
        fs_name: String,
    ) -> Result<(), Error> {
        let mut root_path = mount_path.clone();
        root_path.push("mkdir");
        // Fs::cleanup(&root_path)?;

        // creating the root directory to generate the benchmark directories inside it
        Fs::make_dir(&root_path)?;

        // let (sender, receiver) = channel();
        let handle =
            std::thread::spawn(move || -> Result<(Vec<f64>, Vec<SystemTime>, u64), Error> {
                let mut times = vec![];
                let mut behaviour = vec![];
                let mut idx = 0;
                let mut ops = 0.0;
                loop {
                    match receiver.try_recv() {
                        Ok(Signal::Stop) => {
                            println!("stop signal received");
                            return Ok((times, behaviour, idx));
                        },
                        Ok(Signal::Fetch(fetch_channel)) => {
                            println!("fetch signal received: ops: {}", ops);
                            // TODO real data should be fetched and send rather than random number
                            fetch_channel.try_send(ops).unwrap();
                            ops = 0.0;
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
                                    ops += 1.0;
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
        sender.send(Signal::Stop);

        Ok(())
    }
}