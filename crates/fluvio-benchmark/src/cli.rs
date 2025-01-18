use clap::Parser;
use anyhow::Result;

use crate::{
    benchmark_driver::BenchmarkDriver,
    config::{ConsumerConfig, ProducerConfig},
};

#[derive(Debug, Parser)]
pub struct BenchmarkOpt {
    #[clap(subcommand)]
    benchmark: Option<BenchmarkMode>,
}
impl BenchmarkOpt {
    pub async fn process(self) -> Result<()> {
        run_benchmarks(self).await?;
        Ok(())
    }
}

#[derive(Debug, Parser)]
pub enum BenchmarkMode {
    /// Use a matrix configuration file
    Matrix {
        /// Path to the configuration file
        #[arg(short, long)]
        config: Option<String>,
    },
    /// Run a producer benchmark
    Producer(ProducerConfig),
    /// Run a consumer benchmark
    Consumer(ConsumerConfig),
}

pub async fn run_benchmarks(opt: BenchmarkOpt) -> Result<()> {
    println!("# Fluvio Benchmark Results");

    if let Some(mode) = opt.benchmark {
        BenchmarkDriver::run_benchmark(mode).await?;
    } else {
        BenchmarkDriver::run_benchmark(BenchmarkMode::Matrix { config: None }).await?;
    }

    println!();
    Ok(())
}
