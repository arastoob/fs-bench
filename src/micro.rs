use byte_unit::Byte;
use crate::{BenchMode, Error};

#[derive(Debug)]
pub struct MicroBench {
    mode: BenchMode,
    runtime: u16,
    io_size: usize,
    iteration: Option<u64>
}

impl MicroBench {
    pub fn new(mode: BenchMode, runtime: u16, io_size: String, iteration: Option<u64>) -> Result<Self, Error> {
        let io_size = Byte::from_str(io_size)?;
        let io_size = io_size.get_bytes() as usize;

        Ok(Self {
            mode,
            runtime,
            io_size,
            iteration
        })
    }
}