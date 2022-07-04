use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::SystemTime;
use byte_unit::{Byte, ByteUnit};
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngCore;
use crate::{Bench, BenchFn, BenchResult, Config, Error, Record, ResultMode};
use crate::format::time_format;
use crate::fs::Fs;
use crate::micro::clear_cache;
use crate::plotter::Plotter;
use crate::progress::Progress;

pub struct Throughput {
    config: Config,
}

impl Bench for Throughput {
    fn new(config: Config) -> Result<Self, Error> {
        Ok(Self { config })
    }

    fn setup(&self, path: &PathBuf, invalidate_cache: bool) -> Result<(), Error> {
        if !path.exists() {
            let style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");
            let bar = ProgressBar::new_spinner();
            bar.set_style(style);
            bar.set_message(format!("setting up {}", Fs::path_to_str(path)?));
            let progress = Progress::start(bar.clone());

            let mut file_size = 1024 * 1024 * 64; // 64 MiB
            let mut idx = 0;
            // generate files with sizes from 64 MiB to 1024 MiB in steps of 64 MiB
            while file_size <= 1024 * 1024 * 1024 {
                let mut rand_buffer = vec![0u8; file_size];
                let mut rng = rand::thread_rng();
                rng.fill_bytes(&mut rand_buffer);

                let mut file_name = path.clone();
                file_name.push(idx.to_string());
                Fs::make_file(&file_name)?.write_all(&mut rand_buffer)?;

                file_size += 1024 * 1024 * 64;
                idx += 1;
            }

            progress.finish_and_clear()?;
        }

        if invalidate_cache {
            clear_cache()?;
        }

        Ok(())
    }

    fn run(&self, _bench_fn: Option<BenchFn>) -> Result<(), Error> {
        sudo::escalate_if_needed()?;

        let progress_style = ProgressStyle::default_bar().template("[{elapsed_precise}] {msg}");

        let throughput_header = ["file_size (MiB)".to_string(), "throughput (MiB/s)".to_string()].to_vec();

        let mut read_plotter = Plotter::new();
        let mut write_plotter = Plotter::new();


        for (idx, mount_path) in self.config.mount_paths.iter().enumerate() {
            let mut root_path = mount_path.clone();
            root_path.push("throughput");
            self.setup(&root_path, true)?;

            let read_throughput = self.throughput(
                BenchFn::Read,
                &root_path,
                &self.config.fs_names[idx],
                progress_style.clone())?;

            let write_throughput = self.throughput(
                BenchFn::Write,
                &root_path,
                &self.config.fs_names[idx],
                progress_style.clone())?;

            let mut read_throughput_results = BenchResult::new(throughput_header.clone());
            read_throughput_results.add_records(read_throughput)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!("{}_read_throughput.csv", self.config.fs_names[idx]));
            read_throughput_results.log(&file_name)?;

            read_plotter.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Throughput,
            )?;

            let mut write_throughput_results = BenchResult::new(throughput_header.clone());
            write_throughput_results.add_records(write_throughput)?;
            let mut file_name = self.config.log_path.clone();
            file_name.push(format!(
                "{}_write_throughput.csv",
                self.config.fs_names[idx]
            ));
            write_throughput_results.log(&file_name)?;

            write_plotter.add_coordinates(
                &file_name,
                Some(self.config.fs_names[idx].clone()),
                &ResultMode::Throughput,
            )?;

        }

        let mut file_name = self.config.log_path.clone();
        file_name.push("read_throughput.svg");
        read_plotter.line_chart(
            Some("File size (MiB)"),
            Some("Throughput (MiB/s)"),
            Some("Read Throughput"),
            true,
            true,
            &file_name,
        )?;

        let mut file_name = self.config.log_path.clone();
        file_name.push("write_throughput.svg");
        write_plotter.line_chart(
            Some("File size (MiB)"),
            Some("Throughput (MiB/s)"),
            Some("Write Throughput"),
            true,
            true,
            &file_name,
        )?;


        println!(
            "results logged to: {}",
            Fs::path_to_str(&self.config.log_path)?
        );

        Ok(())
    }
}

impl Throughput {
    fn throughput(
        &self,
        op: BenchFn,
        path: &PathBuf,
        fs_name: &str,
        style: ProgressStyle,
    ) -> Result<Vec<Record>, Error> {

        let bar = ProgressBar::new_spinner();
        bar.set_style(style);
        bar.set_message(format!("{} throughput ({})", op.to_string(), fs_name));
        let progress = Progress::start(bar);

        let mut size = 1024 * 1024 * 64; // 64 MiB
        let mut throughputs = vec![];

        let start = SystemTime::now();
        // read 64 MiB, 128 MiB, 192 MiB, 256 MiB, 320 MiB,..., 1024 MiB
        let mut idx = 0;
        while size <= 1024 * 1024 * 1024 {
            let mut file_name = path.clone();
            file_name.push(idx.to_string());
            let mut file = Fs::open_file(&file_name)?;
            match op {
                BenchFn::Read => {
                    let mut read_buffer = vec![0u8; size];
                    let start1 = SystemTime::now();
                    match file.read_exact(&mut read_buffer) {
                        Ok(_) => {
                            let end1 = start1.elapsed()?.as_secs_f64();
                            let throughput = size as f64 / end1; // B/s
                            throughputs.push((size, throughput));
                        }
                        Err(e) => {
                            println!("error: {:?}", e);
                        }
                    }
                },
                BenchFn::Write => {
                    let mut rand_content = vec![0u8; size];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut rand_content);

                    let start1 = SystemTime::now();
                    match file.write_all(&mut rand_content) {
                        Ok(_) => {
                            let end1 = start1.elapsed()?.as_secs_f64();
                            let throughput = size as f64 / end1; // B/s
                            throughputs.push((size, throughput));

                        }
                        Err(e) => {
                            println!("error: {:?}", e);
                        }
                    }
                },
                _ => {}
            }

            idx += 1;
            size += 1024 * 1024 * 64;
        }

        let end = start.elapsed()?.as_secs_f64();
        progress.finish()?;

        println!("{:11} {}", "run time:", time_format(end));

        let mut throughput_records = vec![];
        for (size, throughput) in throughputs {
            let size = Byte::from_bytes(size as u128);
            let adjusted_size = size.get_appropriate_unit(true);

            let throughput = Byte::from_bytes(throughput as u128);
            let adjusted_throughput = throughput.get_appropriate_unit(true);
            println!(
                "[{:10} {}/s]",
                adjusted_size.format(0),
                adjusted_throughput.format(3),
            );

            // convert the throughout to MiB
            let adjusted_throughput = match adjusted_throughput.get_unit() {
                ByteUnit::B => {
                    adjusted_throughput.get_value() / (1024f64 * 1024f64)
                },
                ByteUnit::KiB => {
                    adjusted_throughput.get_value() / 1024f64
                },
                ByteUnit::MiB => {
                    adjusted_throughput.get_value()
                }
                ByteUnit::GiB => {
                    adjusted_throughput.get_value() * 1024f64
                },
                _ => {
                    adjusted_throughput.get_value()
                }
            };
            throughput_records.push(vec![adjusted_size.get_value().to_string(), adjusted_throughput.to_string()].into());
        }

        println!();
        Ok(throughput_records)
    }
}