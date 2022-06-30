use clap::Parser;
use fs_bench::error::Error;
use fs_bench::micro::offline::OfflineBench;
use fs_bench::micro::real_time::RealTimeBench;
use fs_bench::strace_workload::StraceWorkloadRunner;
use fs_bench::{Bench, BenchMode};
use std::path::PathBuf;
use fs_bench::micro::BenchFn;

/// A library for benchmarking filesystem operations
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The bench modes which could be micro or workload
    #[clap(short, long)]
    bench_mode: BenchMode,

    /// The I/O size, default: 4 KiB
    #[clap(short, long)]
    io_size: Option<String>,

    /// The fileset's file sizes, default: 10 MiB
    #[clap(short = 'l', long)]
    file_size: Option<String>,

    /// Maximum number of files in a fileset, default: 1000
    #[clap(short = 's', long)]
    fileset_size: Option<usize>,

    /// The running time, default: 60 s
    #[clap(short, long)]
    time: Option<f64>,

    /// The path to the mounted filesystem being benchmarked
    #[clap(short, long)]
    mount: Vec<PathBuf>,

    /// Filesystem name that is being benchmarked
    #[clap(short = 'n', long)]
    fs_name: Vec<String>,

    /// The path to store benchmark results
    #[clap(short = 'p', long)]
    log_path: PathBuf,

    /// The path to the strace log file
    #[clap(short, long, required_if_eq("bench-mode", "strace"))]
    workload: Option<PathBuf>,

    /// The benchmark function to be run in real-time
    #[clap(short = 'f', long, required_if_eq("bench-mode", "realtime"))]
    bench_fn: Option<BenchFn>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let fs_names = args.fs_name.into_iter().collect::<Vec<_>>();
    let mount_paths = args.mount.into_iter().collect::<Vec<_>>();

    if fs_names.len() != mount_paths.len() {
        return Err(Error::InvalidConfig(
            "There should be one fs-name per each mount argument".to_string(),
        ));
    }

    match args.bench_mode {
        BenchMode::Static => {
            OfflineBench::configure(
                args.io_size,
                args.file_size,
                args.fileset_size,
                args.time,
                args.workload,
                mount_paths,
                fs_names,
                args.log_path,
            )?
            .run(None)?;
        }
        BenchMode::RealTime => {
            RealTimeBench::configure(
                args.io_size,
                args.file_size,
                args.fileset_size,
                args.time,
                args.workload,
                mount_paths,
                fs_names,
                args.log_path,
            )?
            .run(args.bench_fn)?;
        }
        BenchMode::Strace => {
            if args.workload.is_none() {
                return Err(Error::InvalidConfig(
                    "a valid strace_path not provided".to_string(),
                ));
            }

            StraceWorkloadRunner::configure(
                args.io_size,
                args.file_size,
                args.fileset_size,
                args.time,
                args.workload,
                mount_paths,
                fs_names,
                args.log_path,
            )?
            .run(None)?;
        }
    }

    Ok(())
}
