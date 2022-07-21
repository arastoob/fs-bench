use crate::format::time_format;
use crate::fs::Fs;
use crate::progress::Progress;
use crate::stats::AnalysedData;
use crate::Error;
use byte_unit::Byte;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{thread_rng, Rng, RngCore};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

pub mod offline;
pub mod real_time;
pub mod throughput;

///
/// Benchmark function that is being run
///
#[derive(Debug, Clone, PartialEq)]
pub enum BenchFn {
    Mkdir,
    Mknod,
    Read,
    ColdRead,
    Write,
    WriteSync,
}

impl FromStr for BenchFn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mkdir" => Ok(BenchFn::Mkdir),
            "mknod" => Ok(BenchFn::Mknod),
            "read" => Ok(BenchFn::Read),
            "cold_read" => Ok(BenchFn::ColdRead),
            "write" => Ok(BenchFn::Write),
            "write_sync" => Ok(BenchFn::WriteSync),
            _ => Err(
                "valid benckmark functions are: mkdir, mknod, read, cold_read, write, write_sync"
                    .to_string(),
            ),
        }
    }
}

impl Display for BenchFn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchFn::Mkdir => write!(f, "mkdir"),
            BenchFn::Mknod => write!(f, "mknod"),
            BenchFn::Read => write!(f, "read"),
            BenchFn::ColdRead => write!(f, "cold_read"),
            BenchFn::Write => write!(f, "write"),
            BenchFn::WriteSync => write!(f, "write_sync"),
        }
    }
}

pub fn micro_setup(
    file_size: usize,
    fileset_size: usize,
    path: &PathBuf,
    invalidate_cache: bool,
) -> Result<(), Error> {
    Fs::cleanup(path)?;
    // creating the root directory to generate the benchmark files inside it
    Fs::make_dir(&path)?;

    let style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");
    let bar = ProgressBar::new_spinner();
    bar.set_style(style);
    bar.set_message(format!("setting up {}", Fs::path_to_str(path)?));
    let progress = Progress::start(bar.clone());

    if path.ends_with("mkdir") || path.ends_with("mknod") {
        // we don't need to setup anything for mkdir and mknod
    } else {
        // create files of size io_size filled with random content
        for file in 0..fileset_size {
            let mut file_name = path.clone();
            file_name.push(file.to_string());

            // each file is filled with random content
            let mut rand_buffer = vec![0u8; file_size];
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut rand_buffer);
            Fs::make_file(&file_name)?.write_all(&mut rand_buffer)?;
        }
    }

    progress.finish_and_clear()?;

    if invalidate_cache {
        clear_cache()?;
    }

    Ok(())
}

// get a random leaf from the input path
pub fn random_leaf(path: &PathBuf) -> Result<PathBuf, Error> {
    let entries = path.read_dir()?.collect::<Vec<_>>();
    if entries.len() == 0 {
        return Ok(path.clone());
    }

    // select one of the directories
    let random = thread_rng().gen_range(0..entries.len());
    //TODO fix the unwrap
    random_leaf(&entries[random].as_ref().unwrap().path())
}

pub fn print_output(iterations: u64, run_time: f64, io_size: usize, analysed_data: &AnalysedData, throughput: bool) {
    println!("{:18} {}", "iterations:", iterations);
    println!("{:18} {}", "run time:", time_format(run_time));
    println!(
        "{:18} [{}, {}]",
        "op time (95% CI):",
        time_format(1f64 / analysed_data.mean_ub),
        time_format(1f64 / analysed_data.mean_lb),
    );

    if throughput {
        let byte_s_lb = Byte::from_bytes((analysed_data.mean_lb * io_size as f64) as u128);
        let byte_s_lb = byte_s_lb.get_appropriate_unit(true);

        let byte_s_ub = Byte::from_bytes((analysed_data.mean_ub * io_size as f64) as u128);
        let byte_s_ub = byte_s_ub.get_appropriate_unit(true);

        println!(
            "{:18} [{}, {}] ([{}/s, {}/s])",
            "ops/s (95% CI):", analysed_data.mean_lb, analysed_data.mean_ub, byte_s_lb, byte_s_ub
        );
    } else {
        println!(
            "{:18} [{}, {}]",
            "ops/s (95% CI):", analysed_data.mean_lb, analysed_data.mean_ub
        );
    }

    println!();
}

pub fn clear_cache() -> Result<(), Error> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(ProgressStyle::default_spinner().template("{spinner} clearing the cache"));
    let progress = Progress::start(spinner);

    // sync the cached content to disk and then invalidate the cache
    let sync_status = std::process::Command::new("sh")
        .arg("-c")
        .arg("sync")
        .output()?;
    let invalidate_cache_status = std::process::Command::new("sh")
        .arg("-c")
        .arg("echo 3 | sudo tee /proc/sys/vm/drop_caches")
        .output()?;

    if !sync_status.status.success() || !invalidate_cache_status.status.success() {
        return Err(Error::Unknown(
            "Could not invalidate the OS cache".to_string(),
        ));
    }

    progress.finish_and_clear()?;
    Ok(())
}
