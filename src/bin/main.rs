use std::error::Error;
use clap::Parser;
use fs_bench::BenchMode;
use fs_bench::data_logger::DataLogger;
use fs_bench::micro::MicroBench;

/// A library for benchmarking filesystem operations
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    /// Type of benchmark to run
    #[clap(short, long)]
    benchmark: BenchMode,

    /// Number of seconds to run the benchmark
    #[clap(short, long, default_value = "60")]
    runtime: u16,

    /// The I/O size
    #[clap(short, long, default_value = "4KiB")]
    size: String,

    /// Number of iterations to repeat the operations
    #[clap(short, long, required_if_eq("benchmark", "behaviour"))] // this argument is required if benchmark = behaviour
    iterations: Option<u64>,

    /// The path to the mounted filesystem being benchmarked
    #[clap(short, long)]
    mount: String,

    /// Filesystem name that is being benchmarked
    #[clap(short, long)]
    fs_name: String,

    /// The path to store benchmark results
    #[clap(short, long)]
    log_path: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let data_logger = DataLogger::new(args.fs_name, args.log_path)?;
    let micro_bench = MicroBench::new(args.benchmark, args.runtime, args.size, args.iterations, args.mount, data_logger)?;

    micro_bench.run()?;

    Ok(())
}
