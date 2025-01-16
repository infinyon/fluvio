use clap::Parser;
use anyhow::Result;

use crate::{
    config::{ConsumerConfig, ProducerConfig},
    benchmark_driver::BenchmarkDriver,
};

#[derive(Debug, Parser)]
pub struct BenchmarkOpt {
    #[clap(subcommand)]
    benchmark: BenchmarkCmd,
}
impl BenchmarkOpt {
    pub async fn process(self) -> Result<()> {
        run_benchmarks(self).await?;
        Ok(())
    }
}

#[derive(Debug, Parser)]
pub enum BenchmarkCmd {
    /// Run a producer benchmark
    Producer(ProducerConfig),
    /// Run a consumer benchmark
    Consumer(ConsumerConfig),
}

pub async fn run_benchmarks(args: BenchmarkOpt) -> Result<()> {
    println!("# Fluvio Benchmark Results");
    BenchmarkDriver::run_benchmark(args.benchmark).await?;
    println!();
    Ok(())
}
