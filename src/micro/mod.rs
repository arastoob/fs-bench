use crate::format::time_format;
use crate::fs::Fs;
use crate::progress::Progress;
use crate::stats::AnalysedData;
use crate::Error;
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use std::io::Write;
use std::path::PathBuf;

pub mod offline;
pub mod real_time;

pub fn micro_setup(io_size: usize, fileset_size: usize, path: &PathBuf) -> Result<(), Error> {
    // cleanup the path if already exist
    Fs::cleanup(path)?;

    // creating the root directory to generate the benchmark files inside it
    Fs::make_dir(&path)?;

    if path.ends_with("mkdir") || path.ends_with("mknod") {
        // we don't need to setup anything for mkdir and mknod
        return Ok(());
    }

    let style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");
    let bar = ProgressBar::new_spinner();
    bar.set_style(style);
    bar.set_message(format!("setting up {}", Fs::path_to_str(path)?));
    let progress = Progress::start(bar.clone());

    for file in 0..fileset_size {
        let mut file_name = path.clone();
        file_name.push(file.to_string());

        // each file is filled with random content
        let mut rand_buffer = vec![0u8; io_size];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut rand_buffer);
        Fs::make_file(&file_name)?.write_all(&mut rand_buffer)?;
    }

    progress.finish_and_clear()?;

    Ok(())
}

pub fn print_output(iterations: u64, run_time: f64, analysed_data: &AnalysedData) {
    println!("{:18} {}", "iterations:", iterations);
    println!("{:18} {}", "run time:", time_format(run_time));
    println!(
        "{:18} [{}, {}]",
        "op time (95% CI):",
        time_format(1f64 / analysed_data.mean_ub),
        time_format(1f64 / analysed_data.mean_lb),
    );
    println!(
        "{:18} [{}, {}]",
        "ops/s (95% CI):", analysed_data.mean_lb, analysed_data.mean_ub
    );
    println!();
}
