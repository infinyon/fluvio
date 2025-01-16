use anyhow::Result;

use crate::{cli::BenchmarkCmd, producer_benchmark::ProducerBenchmark};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_benchmark(cmd: BenchmarkCmd) -> Result<()> {
        match cmd {
            BenchmarkCmd::Producer(config) => {
                ProducerBenchmark::run_benchmark(config).await?;
            }
            BenchmarkCmd::Consumer(_) => {
                println!("consume not implemented");
            }
        }

        Ok(())
    }
}
