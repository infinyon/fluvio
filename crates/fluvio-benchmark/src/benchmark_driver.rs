use std::{fs::File, time::Duration};

use anyhow::Result;
use fluvio_future::timer::sleep;

use crate::{cli::BenchmarkMode, config::config_matrix::Matrix, producer_benchmark::ProducerBenchmark};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_benchmark(mode: BenchmarkMode) -> Result<()> {
        match mode {
            BenchmarkMode::Producer(config) => {
                ProducerBenchmark::run_benchmark(config).await?;
            }
            BenchmarkMode::Consumer(_) => {
                println!("consume not implemented");
            }
            BenchmarkMode::Matrix { config } => {
                let matrix_config = if let Some(path) = config {
                    let file = File::open(&path).expect("file not found");
                    serde_yaml::from_reader::<_, Matrix>(file).expect("deserialization failed")
                } else {
                    crate::config::config_matrix::default_config()
                };
                let benchmarks_configs = matrix_config.generate_configs();
                for benchmark_config in benchmarks_configs {
                    println!("Running benchmark: {:#?}", benchmark_config);
                    match benchmark_config {
                        crate::config::BenchmarkConfig::Producer(producer) => {
                            ProducerBenchmark::run_benchmark(producer).await?;
                        }
                        crate::config::BenchmarkConfig::Consumer(_) => {
                            println!("consume not implemented");
                        }
                    }

                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Ok(())
    }
}
