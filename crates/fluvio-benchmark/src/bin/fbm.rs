use std::{
    fs::{File, self},
    io::{Read, Write},
};
use clap::{arg, Parser};
use pad::PadStr;
use fluvio::Compression;
use fluvio_benchmark::{
    benchmark_config::benchmark_matrix::{
        BenchmarkMatrix, RecordKeyAllocationStrategy, get_config_from_file, get_default_config,
        SharedConfig, FluvioProducerConfig, FluvioConsumerConfig, FluvioTopicConfig,
        BenchmarkLoadConfig,
    },
    benchmark_driver::BenchmarkDriver,
    stats::AllStats,
    BenchmarkError,
};

const HISTORIC_RUN_PATH: &str = "target/benchmark/previous";
const HISTORIC_RUN_DIR: &str = "target/benchmark";

fn main() {
    let args = Args::parse();

    if args.example_config {
        print_example_config();
        return;
    }

    // TODO accept directory of files.
    let matrices = match Args::parse().config {
        Some(path) => get_config_from_file(&path),
        None => get_default_config(),
    };

    let all_stats = AllStats::default();
    let previous = load_previous_stats();

    for matrix in matrices {
        print_divider();
        println!(
            "# Beginning Matrix for: {}",
            matrix.shared_config.matrix_name
        );
        print_divider();

        for settings in matrix.into_iter() {
            println!("{}", settings);
            async_std::task::block_on(BenchmarkDriver::run_benchmark(
                settings.clone(),
                all_stats.clone(),
            ))
            .unwrap();
            async_std::task::block_on(all_stats.print_results(&settings));
            if let Some(other) = previous.as_ref() {
                async_std::task::block_on(all_stats.compare_stats(&settings, other.clone()));
            }
            print_divider();
            println!()
        }
    }
    println!("Note: throughput is based on total produced bytes only");

    async_std::task::block_on(write_stats(all_stats)).unwrap();
}

fn load_previous_stats() -> Option<AllStats> {
    let mut file = File::open(HISTORIC_RUN_PATH).ok()?;
    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer).ok()?;
    AllStats::decode(&buffer).ok()
}

async fn write_stats(stats: AllStats) -> Result<(), BenchmarkError> {
    let encoded = stats.encode().await;
    fs::create_dir_all(HISTORIC_RUN_DIR)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("create output dir: {:?}", e)))?;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(HISTORIC_RUN_PATH)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{:?}", e)))?;
    file.write(&encoded)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{:?}", e)))?;
    Ok(())
}

fn print_example_config() {
    let example_config = BenchmarkMatrix {
        shared_config: SharedConfig {
            matrix_name: "ExampleMatrix".to_string(),
            num_samples: 100,
            millis_between_samples: 500,
            worker_timeout_seconds: 3600,
        },
        producer_config: FluvioProducerConfig {
            batch_size: vec![16000],
            queue_size: vec![100],
            linger_millis: vec![10],
            server_timeout_millis: vec![5000],
            compression: vec![Compression::None],
        },
        consumer_config: FluvioConsumerConfig {
            max_bytes: vec![64000],
        },
        topic_config: FluvioTopicConfig {
            num_partitions: vec![1],
        },
        load_config: BenchmarkLoadConfig {
            num_records_per_producer_worker_per_batch: vec![1000],
            record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
            num_concurrent_producer_workers: vec![1],
            num_concurrent_consumers_per_partition: vec![1],
            record_size: vec![1000],
        },
    };
    println!("{}", serde_yaml::to_string(&example_config).unwrap());
}

fn print_divider() {
    println!("{}", "".pad_to_width_with_char(50, '#'));
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
