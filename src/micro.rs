use crate::data_logger::DataLogger;
use crate::plotter::Plotter;
use crate::{make_dir, make_file, read_file, write_file, BenchMode, BenchResult, Error, Record, cleanup};
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{thread_rng, Rng, RngCore};
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use log::error;
use crate::sample::Sample;

#[derive(Debug)]
pub struct MicroBench {
    mode: BenchMode,
    io_size: usize,
    iteration: Option<u64>,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
}

impl MicroBench {
    pub fn new(
        mode: BenchMode,
        io_size: String,
        iteration: Option<u64>,
        mount_path: PathBuf,
        fs_name: String,
        log_path: PathBuf,
    ) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            mode,
            io_size,
            iteration,
            mount_path,
            fs_name,
            log_path,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        let progress_style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let logger = DataLogger::new(self.fs_name.clone(), self.log_path.clone())?;

        match self.mode {
            BenchMode::OpsPerSecond => {
                // results.add_record(self.mkdir(progress_style.clone())?)?;
                // results.add_record(self.mknod(progress_style.clone())?)?;
                // results.add_record(self.read(progress_style.clone())?)?;
                // results.add_record(self.write(progress_style)?)?;

                // let log_file_name = logger.log(results, &self.mode)?;
                //
                // let plotter = Plotter::parse(PathBuf::from(log_file_name), &self.mode)?;
                // plotter.bar_chart(Some("Operation"), Some("Ops/s"), None)?;
                // println!("results logged to {}", path_to_str(&self.log_path));

            }
            BenchMode::Throughput => {}
            BenchMode::Behaviour => {

                let (mkdir_ops_s, mkdir_behaviour) = self.mkdir(progress_style.clone())?;
                let (mknod_ops_s, mknod_behaviour) = self.mknod(progress_style.clone())?;
                let (read_ops_s, read_behaviour) = self.read(progress_style.clone())?;
                let (write_ops_s, write_behaviour) = self.write(progress_style.clone())?;


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


                let behaviour_header = [
                    "second".to_string(),
                    "ops".to_string(),
                ].to_vec();

                let mut mkdir_behaviour_results = BenchResult::new(behaviour_header.clone());
                mkdir_behaviour_results.add_records(mkdir_behaviour)?;
                let mkdir_log = logger.log(mkdir_behaviour_results, "mkdir")?;
                let plotter = Plotter::parse(PathBuf::from(mkdir_log), &BenchMode::Behaviour)?;
                plotter.line_chart(Some("Time"), Some("Ops/s"), None)?;

                let mut mknod_behaviour_results = BenchResult::new(behaviour_header.clone());
                mknod_behaviour_results.add_records(mknod_behaviour)?;
                let mknod_log = logger.log(mknod_behaviour_results, "mknod")?;
                let plotter = Plotter::parse(PathBuf::from(mknod_log), &BenchMode::Behaviour)?;
                plotter.line_chart(Some("Time"), Some("Ops/s"), None)?;

                let mut read_behaviour_results = BenchResult::new(behaviour_header.clone());
                read_behaviour_results.add_records(read_behaviour)?;
                let read_log = logger.log(read_behaviour_results, "read")?;
                let plotter = Plotter::parse(PathBuf::from(read_log), &BenchMode::Behaviour)?;
                plotter.line_chart(Some("Time"), Some("Ops/s"), None)?;

                let mut write_behaviour_results = BenchResult::new(behaviour_header);
                write_behaviour_results.add_records(write_behaviour)?;
                let write_log = logger.log(write_behaviour_results, "write")?;
                let plotter = Plotter::parse(PathBuf::from(write_log), &BenchMode::Behaviour)?;
                plotter.line_chart(Some("Time"), Some("Ops/s"), None)?;

                println!("results logged to: {}", path_to_str(&logger.log_path));
            }
        }

        Ok(())
    }

    fn mkdir(&self, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mkdir");
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration.unwrap());
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mkdir"));

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        let mut dir = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let start = SystemTime::now();
        while dir <= self.iteration.unwrap() {
            let mut dir_name = root_path.clone();
            dir_name.push(dir.to_string());
            let begin = SystemTime::now();
            match make_dir(&dir_name) {
                Ok(()) => {
                    let end = begin.elapsed().unwrap().as_secs_f64();
                    times.push(end);
                    behaviour.push(SystemTime::now());
                    dir = dir + 1;
                }
                Err(e) => {
                    error!("error: {:?}", e);
                }
            }

            bar.inc(1);
        }

        let end = start.elapsed().unwrap().as_secs_f64();

        bar.finish();

        println!("iterations:    {}", self.iteration.unwrap());
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = 1.0 / mean;
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
                (ops_per_second).to_string(),
            ].to_vec(),
        };

        let behaviour_records  = self.ops_in_window(&behaviour, 20)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn mknod(&self, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("mknod");
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration.unwrap());
        bar.set_style(style);
        bar.set_message(format!("{:5}", "mknod"));

        // creating the root directory to generate the test directories inside it
        make_dir(&root_path)?;

        let mut file = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let start = SystemTime::now();
        while file <= self.iteration.unwrap() {
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let begin = SystemTime::now();
            match make_file(&file_name) {
                Ok(_) => {
                    let end = begin.elapsed().unwrap().as_secs_f64();
                    times.push(end);
                    behaviour.push(SystemTime::now());
                    file = file + 1;
                }
                Err(e) => {
                    error!("error: {:?}", e);
                }
            }

            bar.inc(1);
        }

        let end = start.elapsed().unwrap().as_secs_f64();

        bar.finish();

        println!("iterations:    {}", self.iteration.unwrap());
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = 1.0 / mean;
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
                (ops_per_second).to_string(),
            ].to_vec(),
        };

        let behaviour_records  = self.ops_in_window(&behaviour, 20)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn read(&self, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("read");
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration.unwrap());
        bar.set_style(style);
        bar.set_message(format!("{:5}", "read"));

        // creating the root directory to generate the test directories inside it
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

        let start = SystemTime::now();
        while idx <= self.iteration.unwrap() {
            let file = thread_rng().gen_range(1..1001);
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let begin = SystemTime::now();
            match read_file(&file_name, &mut read_buffer) {
                Ok(_) => {
                    let end = begin.elapsed().unwrap().as_secs_f64();
                    times.push(end);
                    behaviour.push(SystemTime::now());
                    idx += 1;
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }

            bar.inc(1);
        }

        let end = start.elapsed().unwrap().as_secs_f64();

        bar.finish();

        println!("iterations:    {}", self.iteration.unwrap());
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = 1.0 / mean;
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
                (ops_per_second).to_string(),
            ].to_vec(),
        };

        let behaviour_records  = self.ops_in_window(&behaviour, 10)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    fn write(&self, style: ProgressStyle) -> Result<(Record, Vec<Record>), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("write");
        cleanup(&root_path)?;

        let bar = ProgressBar::new(self.iteration.unwrap());
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

        let start = SystemTime::now();
        while idx <= self.iteration.unwrap() {
            let rand_content_index = thread_rng().gen_range(0..(8192 * size) - size - 1);
            let mut content =
                rand_content[rand_content_index..(rand_content_index + size)].to_vec();

            let file = thread_rng().gen_range(1..1001);
            let mut file_name = root_path.clone();
            file_name.push(file.to_string());
            let begin = SystemTime::now();
            match write_file(&file_name, &mut content) {
                Ok(_) => {
                    let end = begin.elapsed().unwrap().as_secs_f64();
                    times.push(end);
                    behaviour.push(SystemTime::now());
                    idx += 1;
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }

            bar.inc(1);
        }

        let end = start.elapsed().unwrap().as_secs_f64();

        bar.finish();

        println!("iterations:    {}", self.iteration.unwrap());
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = 1.0 / mean;
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
                (ops_per_second).to_string(),
            ].to_vec(),
        };

        let behaviour_records  = self.ops_in_window(&behaviour, 10)?;

        println!();
        Ok((ops_per_second_record, behaviour_records))
    }

    // count the number of operations in a time window
    // the time window length is in milliseconds
    // the input times contains the timestamps in unix_time format. The first 10 digits are
    // date and time in seconds and the last 9 digits show the milliseconds
    fn ops_in_window(&self, times: &Vec<SystemTime>, window: u64) -> Result<Vec<Record>, Error> {
        let len = times.len();
        let first = times[0]; // first timestamp
        let last = times[len - 1]; // last timestamp

        let mut records = vec![];

        let mut next = first.add(Duration::from_millis(window));
        let mut idx = 0;
        let mut ops = 0;
        while next < last {
            while times[idx] < next { // count ops in this time window
                ops += 1;
                idx += 1;
            }
            let time = next.duration_since(first).unwrap().as_secs_f64();
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string()
                ].to_vec()
            };
            records.push(record);

            // go the next time window
            next = next.add(Duration::from_millis(window));
            ops = 0;
        }

        // count the remaining
        if idx < len {
            ops = len - idx;
            let time = last.duration_since(first).unwrap().as_secs_f64();
            let record = Record {
                fields: [
                    time.to_string(),
                    // we have counted ops in a window length milliseconds, so the ops in
                    // a second is (ops * 1000) / window
                    ((ops * 1000) / window as usize).to_string()
                ].to_vec()
            };
            records.push(record);
        }

        Ok(records)
    }
}

fn path_to_str(path: &PathBuf) -> &str {
    path.as_os_str().to_str().unwrap()
}
