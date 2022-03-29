use clap::Parser;
use std::path::PathBuf;
use fs_bench::BenchMode;
use fs_bench::error::Error;
use fs_bench::micro::MicroBench;
use fs_bench::workload::WorkloadRunner;

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

    /// The path to the .wasm file including the workload
    /// The .wasm file should include two functions:
    ///     setup: prepare the workload environment
    ///     run: execute the workload
    /// Both setup and run functions take two i32 inputs, which specify the start address and
    /// the length of the base path string. The base path is the path to the directory in which
    /// the workload sub-directories/files are generated.
    #[clap(short, long, required_if_eq("bench_mode", "workload"))]
    wasm_path: Option<PathBuf>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    match args.bench_mode {
        BenchMode::Micro => {
            let micro_bench = MicroBench::new(
                args.size,
                args.iterations,
                args.mount,
                args.fs_name,
                args.log_path,
            )?;
            micro_bench.run()?;
        },
        BenchMode::Workload => {
            let wasm_path = match args.wasm_path {
                Some(wasm_path) => wasm_path,
                None => return Err(Error::InvalidConfig("a valid wasm_path not provided".to_string()))
            };
            let workload_runner = WorkloadRunner::new(
                args.iterations,
                args.mount,
                args.fs_name,
                args.log_path,
                wasm_path
            )?;
            workload_runner.run()?;
        }
    }

    Ok(())
}
