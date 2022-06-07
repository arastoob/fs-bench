use crate::format::{percent_format, time_format};
use crate::sample::AnalysedData;

pub mod offline;
pub mod real_time;


pub fn print_output(iterations: u64, run_time: f64, analysed_data: &AnalysedData) {
    println!("{:18} {}", "iterations:", iterations);
    println!("{:18} {}", "run time:", time_format(run_time));
    println!(
        "{:18} [{}, {}]",
        "op time (95% CI):",
        time_format(analysed_data.mean_lb),
        time_format(analysed_data.mean_ub),
    );
    println!(
        "{:18} [{}, {}]",
        "ops/s (95% CI):", analysed_data.ops_per_second_lb, analysed_data.ops_per_second_ub
    );
    println!(
        "{:18} {}",
        "outliers:",
        percent_format(analysed_data.outliers_percentage)
    );
    println!();
}