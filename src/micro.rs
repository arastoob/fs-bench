use crate::data_logger::DataLogger;
use crate::plotter::Plotter;
use crate::sample::Sample;
use crate::timer::Timer;
use crate::{BenchResult, Error, Fs, Record, ResultMode};
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use rand::{thread_rng, Rng, RngCore};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct MicroBench {
    io_size: usize,
    iteration: u64,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
}

impl MicroBench {
    pub fn new(
        io_size: String,
        iteration: u64,
        mount_path: PathBuf,
        fs_name: String,
        log_path: PathBuf,
    ) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            io_size,
            iteration,
            mount_path,
            fs_name,
            log_path,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        let max_rt = Duration::from_secs(60 * 5); // maximum running time

        self.behaviour_bench(max_rt)?;
        self.throughput_bench(max_rt)?;

        println!("results logged to: {}", Fs::path_to_str(&self.log_path)?);

        Ok(())
    }

    fn behaviour_bench(&self, max_rt: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let (mkdir_ops_s, mkdir_behaviour) = self.mkdir(max_rt, progress_style.clone())?;
        let (mknod_ops_s, mknod_behaviour) = self.mknod(max_rt, progress_style.clone())?;
        let (read_ops_s, read_behaviour) = self.read(max_rt, progress_style.clone())?;
        let (write_ops_s, write_behaviour) = self.write(max_rt, progress_style.clone())?;

        let ops_s_header = [
            "operation".to_string(),
            "runtime(s)".to_string(),
            "ops/s".to_string(),
        ]
        .to_vec();
        let mut ops_s_results = BenchResult::new(ops_s_header);

        ops_s_results.add_record(mkdir_ops_s)?;
        ops_s_results.add_record(mknod_ops_s)?;
        ops_s_results.add_record(read_ops_s)?;
        ops_s_results.add_record(write_ops_s)?;

        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_ops_per_second.csv", self.fs_name));
        DataLogger::log(ops_s_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::OpsPerSecond)?;
        file_name.set_extension("svg");
        plotter.bar_chart(Some("Operation"), Some("Ops/s"), None, &file_name)?;

        let behaviour_header = ["second".to_string(), "ops".to_string()].to_vec();
        let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
        mkdir_behaviour_results.add_records(mkdir_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mkdir.csv", self.fs_name));
        DataLogger::log(mkdir_behaviour_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None, &ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;


        let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
        mknod_behaviour_results.add_records(mknod_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_mknod.csv", self.fs_name));
        DataLogger::log(mknod_behaviour_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None,&ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
        read_behaviour_results.add_records(read_behaviour)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_read.csv", self.fs_name));
        DataLogger::log(read_behaviour_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None,&ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        let mut write_behaviour_results = BenchResult::new(behaviour_header);
        write_behaviour_results.add_records(write_behaviour)?;
        // let write_log = logger.log(write_behaviour_results, "write")?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_write.csv", self.fs_name));
        DataLogger::log(write_behaviour_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None,&ResultMode::Behaviour)?;
        file_name.set_extension("svg");
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false, &file_name)?;

        Ok(())
    }

    fn throughput_bench(&self, max_rt: Duration) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let throughput_header = ["file_size".to_string(), "throughput".to_string()].to_vec();

        let read_throughput = self.read_throughput(max_rt, progress_style.clone())?;
        let write_throughput = self.write_throughput(max_rt, progress_style.clone())?;

        let mut read_throughput_results = BenchResult::new(throughput_header.clone());
        read_throughput_results.add_records(read_throughput)?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_read_throughput.csv", self.fs_name));
        DataLogger::log(read_throughput_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None,&ResultMode::Throughput)?;
        file_name.set_extension("svg");
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
            &file_name
        )?;

        let mut write_throughput_results = BenchResult::new(throughput_header);
        write_throughput_results.add_records(write_throughput)?;
        // let write_throughput_log = logger.log(write_throughput_results, "write_throughput")?;
        let mut file_name = self.log_path.clone();
        file_name.push(format!("{}_write_throughput.csv", self.fs_name));
        DataLogger::log(write_throughput_results, &file_name)?;

        let mut plotter = Plotter::new();
        plotter.add_coordinates(&file_name, None,&ResultMode::Throughput)?;
        file_name.set_extension("svg");
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
            &file_name
        )?;

        Ok(())
    }

    fn mkdir(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mkdir");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mkdir"));

        // creating the root directory to generate the test directories inside it
        Fs::make_dir(&root_path)?;

        let mut idx = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;
        let start = SystemTime::now();
        while idx <= self.iteration {
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

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("mkdir exceeded the max runtime");
        }

        println!("iterations:    {}", idx - 1);
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = (1.0 / mean).floor();
        println!("mean:          {}", mean);
        println!("ops/s:         {}", ops_per_second);
        println!("op time:       {} s", mean as f64 / 1.0);

        let outliers = sample.outliers();
        let outliers_percentage = (outliers.len() as f64 / times.len() as f64) * 100f64;
        println!("outliers:      {} %", outliers_percentage);

        let ops_per_second_record = Record {
            fields: [
                "mkdir".to_string(),
                end.to_string(),
                ops_per_second.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn mknod(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mknod");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mknod"));

        // creating the root directory to generate the test directories inside it
        Fs::make_dir(&root_path)?;

        let mut idx = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;

        let start = SystemTime::now();
        while idx <= self.iteration {
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

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("mknod exceeded the max runtime");
        }

        println!("iterations:    {}", idx - 1);
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = (1.0 / mean).floor();
        println!("mean:          {}", mean);
        println!("ops/s:         {}", ops_per_second);
        println!("op time:       {} s", mean as f64 / 1.0);

        let outliers = sample.outliers();
        let outliers_percentage = (outliers.len() as f64 / times.len() as f64) * 100f64;
        println!("outliers:      {} %", outliers_percentage);

        let ops_per_second_record = Record {
            fields: [
                "mknod".to_string(),
                end.to_string(),
                ops_per_second.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn read(&self, max_rt: Duration, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read"));

        // creating the root directory to generate the test files inside it
        Fs::make_dir(&root_path)?;

        let size = self.io_size;
        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let mut file = Fs::make_file(&file_name)?;

            // generate a buffer of size write_size filled with random integer values
            let mut rand_buffer = vec![0u8; size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_buffer);

            file.write(&rand_buffer)?;
        }

        let mut idx = 0;
        let mut times = vec![];
        let mut behaviour = vec![];
        let mut read_buffer = vec![0u8; size];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;

        let start = SystemTime::now();
        while idx <= self.iteration {
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

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("read exceeded the max runtime");
        }

        println!("iterations:    {}", idx - 1);
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = (1.0 / mean).floor();
        println!("mean:          {}", mean);
        println!("ops/s:         {}", ops_per_second);
        println!("op time:       {} s", mean as f64 / 1.0);

        let outliers = sample.outliers();
        let outliers_percentage = (outliers.len() as f64 / times.len() as f64) * 100f64;
        println!("outliers:      {} %", outliers_percentage);

        let ops_per_second_record = Record {
            fields: [
                "read".to_string(),
                end.to_string(),
                ops_per_second.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn write(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("write");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "write"));

        // creating the root directory to generate the test directories inside it
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

        let mut idx = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;

        let start = SystemTime::now();
        while idx <= self.iteration {
            let rand_content_index = thread_rng().gen_range(0..(8192 * size) - size - 1);
            let mut content =
                rand_content[rand_content_index..(rand_content_index + size)].to_vec();

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

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("write exceeded the max runtime");
        }

        println!("iterations:    {}", idx - 1);
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = (1.0 / mean).floor();
        println!("mean:          {}", mean);
        println!("ops/s:         {}", ops_per_second);
        println!("op time:       {} s", mean as f64 / 1.0);

        let outliers = sample.outliers();
        let outliers_percentage = (outliers.len() as f64 / times.len() as f64) * 100f64;
        println!("outliers:      {} %", outliers_percentage);

        let ops_per_second_record = Record {
            fields: [
                "write".to_string(),
                end.to_string(),
                ops_per_second.to_string(),
            ]
            .to_vec(),
        };

        let behaviour_records = Fs::ops_in_window(&behaviour)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn read_throughput(
        &self,
        max_rt: Duration,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        Fs::cleanup(&root_path)?;

        let bar = ProgressBar::new(6);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read_throughput"));

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

            let sample = Sample::new(&times);
            let mean = sample.mean();
            let throughput = read_size as f64 / mean; // B/s
            throughputs.push((read_size, throughput));
            read_size *= 10;

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("read exceeded the max runtime");
        }

        println!("run time:      {}", end);

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

        let bar = ProgressBar::new(6);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "write_throughput"));

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

            let sample = Sample::new(&times);
            let mean = sample.mean();
            let throughput = write_size as f64 / mean; // B/s
            throughputs.push((write_size, throughput));
            write_size *= 10;

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("read exceeded the max runtime");
        }

        println!("run time:      {}", end);

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
}
