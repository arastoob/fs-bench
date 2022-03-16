use clap::Parser;
use fs_bench::micro::MicroBench;
use std::error::Error;
use std::path::PathBuf;

/// A library for benchmarking filesystem operations
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The I/O size
    #[clap(short, long, default_value = "4KiB")]
    size: String,

    /// Number of iterations to repeat the operations
    #[clap(short, long, default_value = "100000")]
    iterations: u64,

    /// The path to the mounted filesystem being benchmarked
    #[clap(short, long)]
    mount: PathBuf,

    /// Filesystem name that is being benchmarked
    #[clap(short, long)]
    fs_name: String,

    /// The path to store benchmark results
    #[clap(short, long)]
    log_path: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let micro_bench = MicroBench::new(
        args.size,
        args.iterations,
        args.mount,
        args.fs_name,
        args.log_path,
    )?;

    micro_bench.run()?;

    Ok(())
}
