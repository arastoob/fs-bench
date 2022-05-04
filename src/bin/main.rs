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

    /// The path to the mounted filesystem being benchmarked
    #[clap(short, long)]
    mount: PathBuf,

    /// Filesystem name that is being benchmarked
    #[clap(short, long)]
    fs_name: String,

    /// The path to store benchmark results
    #[clap(short, long)]
    log_path: PathBuf,

    /// The path to the strace log file
    #[clap(short, long, required_if_eq("bench_mode", "strace"))]
    workload: Option<PathBuf>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    match args.bench_mode {
        BenchMode::Micro => {
            let micro_bench = MicroBench::new(
                args.size,
                args.mount,
                args.fs_name,
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
            let mut strace_workload = StraceWorkloadRunner::new(
                args.mount,
                args.fs_name,
                args.log_path,
                strace_path,
            )?;
            strace_workload.replay()?;
        }
    }

    Ok(())
}
