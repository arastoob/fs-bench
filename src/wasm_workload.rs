use crate::data_logger::DataLogger;
use crate::plotter::Plotter;
use crate::sample::Sample;
use crate::timer::Timer;
use crate::{BenchResult, Error, Fs, ResultMode};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use wasmer::{Instance, Module, Store};
use wasmer_wasi::WasiState;

/// The .wasm workload runner
/// The .wasm file should include two functions:
///     setup: prepare the workload environment
///     run: execute the workload
/// Both setup and run functions take two i32 inputs, which specify the start address and
/// the length of the base path string. The base path is the path to the directory in which
/// the workload sub-directories/files are generated.
#[derive(Debug)]
pub struct WasmWorkloadRunner {
    iteration: u64,
    mount_path: PathBuf,
    fs_name: String,
    log_path: PathBuf,
    wasm_path: PathBuf,
}

impl WasmWorkloadRunner {
    pub fn new(
        iteration: u64,
        mount_path: PathBuf,
        fs_name: String,
        log_path: PathBuf,
        wasm_path: PathBuf,
    ) -> Result<Self, Error> {
        Ok(Self {
            iteration,
            mount_path,
            fs_name,
            log_path,
            wasm_path,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        let mut root_path = self.mount_path.clone();
        root_path.push("workload");
        Fs::cleanup(&root_path)?;

        let style = ProgressStyle::default_bar()
            .template("{msg} [{bar:40}]")
            .progress_chars("=> ");

        let bar = ProgressBar::new(self.iteration);
        bar.set_style(style);
        bar.set_message(format!("{:5}", "workload"));

        // creating the root directory to generate the workload inside it
        Fs::make_dir(&root_path)?;

        let logger = DataLogger::new(self.fs_name.clone(), self.log_path.clone())?;
        let max_rt = Duration::from_secs(60 * 5); // maximum running time

        let instance = self.wasm_instance()?;

        // Write the mount_path string into the lineary memory
        let root_path = Fs::path_to_str(&root_path)?;
        let memory = instance
            .exports
            .get_memory("memory")
            .map_err(|err| Error::WasmerError(format!("{:?}", err)))?;

        for (byte, cell) in root_path
            .bytes()
            .zip(memory.view()[0 as usize..(root_path.len()) as usize].iter())
        {
            cell.set(byte);
        }

        // make sure both setup and run functions exist
        let setup = instance
            .exports
            .get_native_function::<(i32, i32), ()>("setup")
            .map_err(|err| Error::WasmerError(format!("{:?}", err)))?;
        let run = instance
            .exports
            .get_native_function::<(i32, i32), ()>("run")
            .map_err(|err| Error::WasmerError(format!("{:?}", err)))?;

        // call the setup function
        setup
            .call(0, root_path.len() as i32)
            .map_err(|err| Error::WasmerError(format!("{:?}", err)))?;

        // iterate the run function and produce the ops/s and behaviour results/plots

        let mut idx = 0;
        let mut times = vec![];
        let mut behaviour = vec![];

        let timer = Timer::new(max_rt);
        timer.start();
        let mut interrupted = false;
        let start = SystemTime::now();
        let len = root_path.len() as i32;
        while idx <= self.iteration {
            let begin = SystemTime::now();
            // call the run function
            match run.call(0, len) {
                Ok(_) => {
                    let end = begin.elapsed()?.as_secs_f64();
                    times.push(end);
                    behaviour.push(SystemTime::now());
                    idx = idx + 1;
                }
                Err(err) => {
                    return Err(Error::WasmerError(format!("{:?}", err)));
                }
            }

            bar.inc(1);

            if timer.finished() {
                interrupted = true;
                break;
            }
        }

        let end = start.elapsed()?.as_secs_f64();

        if !interrupted {
            bar.finish();
        } else {
            bar.abandon_with_message("mkdir exceeded the max runtime");
        }

        println!("iterations:    {}", idx - 1);
        println!("run time:      {}", end);

        let sample = Sample::new(&times);
        let mean = sample.mean();
        let ops_per_second = (1.0 / mean).floor();
        println!("mean:          {}", mean);
        println!("ops/s:         {}", ops_per_second);
        println!("op time:       {} s", mean as f64 / 1.0);

        let outliers = sample.outliers();
        let outliers_percentage = (outliers.len() as f64 / times.len() as f64) * 100f64;
        println!("outliers:      {} %", outliers_percentage);

        let behaviour_records = Fs::ops_in_window(&behaviour)?;

        // generate the behaviour plots
        let behaviour_header = ["second".to_string(), "ops".to_string()].to_vec();
        let mut workload_behaviour_results = BenchResult::new(behaviour_header.clone());
        workload_behaviour_results.add_records(behaviour_records)?;
        let workload_log = logger.log(workload_behaviour_results, "workload")?;
        let plotter = Plotter::parse(PathBuf::from(workload_log), &ResultMode::Behaviour)?;
        plotter.line_chart(Some("Time"), Some("Ops/s"), None, false, false)?;

        // // call the run function
        // run.call(0, root_path.len() as i32)
        //     .map_err(|err| Error::WasmerError(format!("{:?}", err)))?;

        Ok(())
    }

    // get a wasm instance to run the wasm functions
    fn wasm_instance(&self) -> Result<Instance, Box<dyn std::error::Error>> {
        let wasm_bytes = std::fs::read(self.wasm_path.clone())?;
        let store = Store::default();
        let module = Module::new(&store, wasm_bytes)?;

        let mut wasi_env = WasiState::new("fs-bench")
            .preopen(|p| {
                p.directory(self.mount_path.clone())
                    .read(true)
                    .write(true)
                    .create(true)
            })?
            .finalize()?;

        let import_object = wasi_env.import_object(&module)?;
        let instance = Instance::new(&module, &import_object)?;

        Ok(instance)
    }
}
