use crate::data_logger::DataLogger;
use crate::plotter::Plotter;
use crate::sample::Sample;
use crate::timer::Timer;
use crate::{cleanup, make_dir, make_file, read_file, read_file_at, write_file, BenchMode, BenchResult, Error, Record, path_to_str};
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use rand::{thread_rng, Rng, RngCore};
use std::io::Write;
use std::ops::Add;
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
        let logger = DataLogger::new(self.fs_name.clone(), self.log_path.clone())?;
        let max_rt = Duration::from_secs(60 * 5); // maximum running time

        self.behaviour_bench(max_rt, logger.clone())?;
        self.throughput_bench(max_rt, logger.clone())?;

        println!("results logged to: {}", path_to_str(&logger.log_path)?);

        Ok(())
    }

    fn behaviour_bench(&self, max_rt: Duration, logger: DataLogger) -> Result<(), Error> {
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
        let ops_s_log = logger.log(ops_s_results, "ops_per_second")?;
        let plotter = Plotter::parse(PathBuf::from(ops_s_log), &BenchMode::OpsPerSecond)?;
        plotter.bar_chart(Some("Operation"), Some("Ops/s"), None)?;

        let behaviour_header = ["second".to_string(), "ops".to_string()].to_vec();

        let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
        mkdir_behaviour_results.add_records(mkdir_behaviour)?;
        let mkdir_log = logger.log(mkdir_behaviour_results, "mkdir")?;
        let plotter = Plotter::parse(PathBuf::from(mkdir_log), &BenchMode::Behaviour)?;
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false)?;

        let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
        mknod_behaviour_results.add_records(mknod_behaviour)?;
        let mknod_log = logger.log(mknod_behaviour_results, "mknod")?;
        let plotter = Plotter::parse(PathBuf::from(mknod_log), &BenchMode::Behaviour)?;
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false)?;

        let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
        read_behaviour_results.add_records(read_behaviour)?;
        let read_log = logger.log(read_behaviour_results, "read")?;
        let plotter = Plotter::parse(PathBuf::from(read_log), &BenchMode::Behaviour)?;
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false)?;

        let mut write_behaviour_results = BenchResult::new(behaviour_header);
        write_behaviour_results.add_records(write_behaviour)?;
        let write_log = logger.log(write_behaviour_results, "write")?;
        let plotter = Plotter::parse(PathBuf::from(write_log), &BenchMode::Behaviour)?;
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false)?;

        Ok(())
    }

    fn throughput_bench(&self, max_rt: Duration, logger: DataLogger) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let throughput_header = ["file_size".to_string(), "throughput".to_string()].to_vec();

        let read_throughput = self.read_throughput(max_rt, progress_style.clone())?;
        let write_throughput = self.write_throughput(max_rt, progress_style.clone())?;

        let mut read_throughput_results = BenchResult::new(throughput_header.clone());
        read_throughput_results.add_records(read_throughput)?;
        let read_throughput_log = logger.log(read_throughput_results, "read_throughput")?;
        let plotter = Plotter::parse(PathBuf::from(read_throughput_log), &BenchMode::Throughput)?;
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
        )?;

        let mut write_throughput_results = BenchResult::new(throughput_header);
        write_throughput_results.add_records(write_throughput)?;
        let write_throughput_log = logger.log(write_throughput_results, "write_throughput")?;
        let plotter = Plotter::parse(PathBuf::from(write_throughput_log), &BenchMode::Throughput)?;
        plotter.line_chart(
            Some("File size [B]"),
            Some("Throughput [B/s]"),
            None,
            true,
            true,
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
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mkdir"));

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

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
            match make_dir(&dir_name) {
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

        let behaviour_records = self.ops_in_window(&behaviour)?;

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
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mknod"));

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

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
            match make_file(&file_name) {
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

        let behaviour_records = self.ops_in_window(&behaviour)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn read(&self, max_rt: Duration, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read"));

        // creating the root directory to generate the test files inside it
        make_dir(&root_path)?;

        let size = self.io_size;
        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let mut file = make_file(&file_name)?;

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
            match read_file(&file_name, &mut read_buffer) {
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

        let behaviour_records = self.ops_in_window(&behaviour)?;

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
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "write"));

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        for file in 1..1001 {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            make_file(&file_name)?;
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
            match write_file(&file_name, &mut content) {
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

        let behaviour_records = self.ops_in_window(&behaviour)?;

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
        cleanup(&root_path)?;

        let bar = ProgressBar::new(6);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read_throughput"));

        // creating the root directory to generate the test files inside it
        make_dir(&root_path)?;

        // create a big file filled with random content
        let mut file_name = root_path.clone();
        file_name.push("big_file".to_string());
        let mut file = make_file(&file_name)?;

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
                match read_file_at(&file_name, &mut read_buffer, rand_index) {
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
        cleanup(&root_path)?;

        let bar = ProgressBar::new(6);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "write_throughput"));

        // creating the root directory to generate the test files inside it
        make_dir(&root_path)?;

        // create a file to write into
        let mut file_name = root_path.clone();
        file_name.push("big_file".to_string());
        make_file(&file_name)?;

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
                match write_file(&file_name, &mut content) {
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

    // count the number of operations in a time window
    // the time window length is in milliseconds
    // the input times contains the timestamps in unix_time format. The first 10 digits are
    // date and time in seconds and the last 9 digits show the milliseconds
    fn ops_in_window(&self, times: &Vec<SystemTime>) -> Result<Vec<Record>, Error> {
        let len = times.len();
        let first = times[0]; // first timestamp
        let last = times[len - 1]; // last timestamp

        // decide about the window length in millis
        let duration = last.duration_since(first)?.as_secs_f64();
        let window = if duration < 0.5 {
            2
        } else if duration < 1f64 {
            5
        } else if duration < 3f64 {
            10
        } else if duration < 5f64 {
            20
        } else if duration < 10f64 {
            50
        } else if duration < 20f64 {
            70
        }  else if duration < 50f64 {
            100
        } else if duration < 100f64 {
            150
        } else if duration < 150f64 {
            200
        } else if duration < 200f64 {
            500
        } else if duration < 300f64 {
            1000
        } else  {
            5000
        };


        let mut records = vec![];

        let mut next = first.add(Duration::from_millis(window));
        let mut idx = 0;
        let mut ops = 0;
        while next < last {
            while times[idx] < next {
                // count ops in this time window
                ops += 1;
                idx += 1;
            }
            let time = next.duration_since(first)?.as_secs_f64();
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string(),
                ]
                .to_vec(),
            };
            records.push(record);

            // go the next time window
            next = next.add(Duration::from_millis(window));
            ops = 0;
        }

        // count the remaining
        if idx < len {
            ops = len - idx;
            let time = last.duration_since(first)?.as_secs_f64();
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string(),
                ]
                .to_vec(),
            };
            records.push(record);
        }

        Ok(records)
    }
}
