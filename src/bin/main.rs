use std::error::Error;
use byte_unit::Byte;
use clap::Parser;
use fs_bench::BenchMode;
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
    #[clap(short, long, required_if("benchmark", "behaviour"))] // this argument is required if benchmark = behaviour
    iterations: Option<u64>
}

fn main() -> Result<(), Box<dyn Error>> {
    let opt = Args::from_args();

    let micro_bench = MicroBench::new(opt.benchmark, opt.runtime, opt.size, opt.iterations)?;

    println!("micro_bench: {:?}", micro_bench);

    Ok(())
}
