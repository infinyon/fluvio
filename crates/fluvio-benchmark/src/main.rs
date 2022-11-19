use clap::{arg, Parser};
use fluvio::Compression;
use fluvio_benchmark::benchmark_config::benchmark_matrix::{
    BenchmarkMatrix, RecordKeyAllocationStrategy, RecordSizeStrategy,
};

fn main() {
    let args = Args::parse();

    if args.example_config {
        print_example_config();
        return;
    }
}

fn print_example_config() {
    let example_config = BenchmarkMatrix {
        num_samples: 5,
        num_batches_per_sample: 10,
        seconds_between_batches: 5,
        worker_timeout_seconds: 10,
        num_records_per_producer_worker_per_batch: vec![1000],
        producer_batch_size: vec![16000, 32000],
        producer_queue_size: vec![100],
        producer_linger_millis: vec![0, 10],
        producer_server_timeout_millis: vec![5000],
        producer_compression: vec![Compression::None, Compression::Gzip],
        record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
        consumer_max_bytes: vec![64000],
        num_concurrent_producer_workers: vec![1],
        num_concurrent_consumers_per_partition: vec![1],
        num_partitions: vec![1],
        record_size_strategy: vec![RecordSizeStrategy::Fixed(1000)],
    };
    println!("{}", serde_yaml::to_string(&example_config).unwrap());
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to a config file to run. If not found, looks for config files in crates/fluvio-benchmark/benches/
    #[arg(short, long)]
    config: Option<String>,

    /// Print out an example config file and then exit
    #[arg(short, long)]
    example_config: bool,
}
