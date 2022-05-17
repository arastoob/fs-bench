use clap::Parser;
use fs_bench::error::Error;
use fs_bench::micro::MicroBench;
use fs_bench::strace_workload::StraceWorkloadRunner;
use fs_bench::BenchMode;
use std::path::PathBuf;

/// A library for benchmarking filesystem operations
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The bench modes which could be micro or workload
    #[clap(short, long)]
    bench_mode: BenchMode,

    /// The I/O size
    #[clap(short, long, default_value = "4KiB")]
    size: String,

    /// The running time with default value of 60
    #[clap(short, long, default_value = "60")]
    time: f64,

    /// The path to the mounted filesystem being benchmarked
    #[clap(short, long)]
    mount: Vec<PathBuf>,

    /// Filesystem name that is being benchmarked
    #[clap(short, long)]
    fs_name: Vec<String>,

    /// The path to store benchmark results
    #[clap(short, long)]
    log_path: PathBuf,

    /// The path to the strace log file
    #[clap(short, long, required_if_eq("bench_mode", "strace"))]
    workload: Option<PathBuf>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let fs_names = args.fs_name.into_iter().collect::<Vec<_>>();
    let mount_paths = args.mount.into_iter().collect::<Vec<_>>();

    if fs_names.len() != mount_paths.len() {
        return Err(Error::InvalidConfig("There should be one fs-name per each mount argument".to_string()));
    }

    match args.bench_mode {
        BenchMode::Micro => {
            let micro_bench = MicroBench::new(
                args.size,
                args.time,
                mount_paths,
                fs_names,
                args.log_path,
            )?;
            micro_bench.run()?;
        }
        BenchMode::Strace => {
            let strace_path = match args.workload {
                Some(strace_path) => strace_path,
                None => {
                    return Err(Error::InvalidConfig(
                        "a valid strace_path not provided".to_string(),
                    ))
                }
            };
            let mut strace_workload =
                StraceWorkloadRunner::new(mount_paths, fs_names, args.log_path, strace_path)?;
            strace_workload.replay()?;
        }
    }

    Ok(())
}
