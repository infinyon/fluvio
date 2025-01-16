use anyhow::Result;
use clap::Parser;
use fluvio_benchmark::cli::{BenchmarkOpt, run_benchmarks};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_logger();
    let args = BenchmarkOpt::parse();

    run_block_on(run_benchmarks(args))
}
