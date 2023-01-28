use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
    mem,
    time::Duration,
};

use clap::{arg, Parser};
use anyhow::Result;

use fluvio_cli_common::install::fluvio_base_dir;
use fluvio_future::{task::run_block_on, sync::Mutex, future::timeout};
use futures_util::FutureExt;
use fluvio::{Compression, Isolation};
use fluvio_benchmark::{
    benchmark_config::{
        benchmark_matrix::{
            BenchmarkMatrix, RecordKeyAllocationStrategy, get_config_from_file, SharedConfig,
            FluvioProducerConfig, FluvioConsumerConfig, FluvioTopicConfig, BenchmarkLoadConfig,
            DeliverySemanticStrategy, AtLeastOnceStrategy,
        },
        Seconds, Millis,
    },
    benchmark_driver::BenchmarkDriver,
    stats::{AllStats, AllStatsSync},
    BenchmarkError,
};

fn main() -> Result<()> {
    fluvio_future::subscriber::init_logger();
    let args = Args::parse();

    if args.example_config {
        print_example_config();
        return Ok(());
    }

    // TODO accept directory of files.
    let matrices = if args.test_cluster {
        test_configs()
    } else {
        match args.config {
            Some(path) => get_config_from_file(&path),
            None => default_configs(),
        }
    };

    let all_stats = Arc::new(Mutex::new(AllStats::default()));
    let previous = load_previous_stats();

    println!("# Fluvio Benchmark Results");
    for matrix in matrices {
        println!("## Matrix: {}", matrix.shared_config.matrix_name);
        for (i, config) in matrix.into_iter().enumerate() {
            run_block_on(timeout(
                // Give time for workers to clean up if workers timeout.
                config.worker_timeout + Duration::from_secs(10),
                BenchmarkDriver::run_benchmark(config.clone(), all_stats.clone()),
            ))??;
            println!("### {}: Iteration {:3.0}", config.matrix_name, i);
            println!("{}", config.to_markdown());
            println!();
            run_block_on(
                all_stats
                    .lock()
                    .map(|a| println!("{}", a.to_markdown(&config))),
            );
            if let Some(other) = previous.as_ref() {
                run_block_on(
                    all_stats
                        .lock()
                        .map(|a| println!("{}", a.compare_stats(&config, other))),
                );
                println!();
            }
        }
    }

    let mut all_stats = run_block_on(take_stats(all_stats));

    if let Some(previous) = previous {
        all_stats.merge(&previous)
    }
    write_stats(all_stats)
}

async fn take_stats(all_stats: AllStatsSync) -> AllStats {
    let mut guard = all_stats.lock().await;
    mem::take(&mut *guard)
}
fn benchmarking_dir() -> Result<PathBuf> {
    let dir_path = fluvio_base_dir()?.join("benchmarks");
    if !dir_path.exists() {
        std::fs::create_dir_all(&dir_path)?;
    }
    Ok(dir_path)
}

fn historic_run_path() -> Result<PathBuf> {
    let mut path = benchmarking_dir()?;
    path.push("previous");
    Ok(path)
}

fn load_previous_stats() -> Option<AllStats> {
    let mut file = File::open(historic_run_path().ok()?).ok()?;
    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer).ok()?;
    AllStats::decode(&buffer).ok()
}

fn write_stats(stats: AllStats) -> Result<()> {
    let encoded = stats.encode();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(historic_run_path()?)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{e:?}")))?;
    file.write(&encoded)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{e:?}")))?;
    Ok(())
}

fn print_example_config() {
    println!("# This provides examples of all options, it is not recommended for use as is.");
    let example_config = BenchmarkMatrix {
        shared_config: SharedConfig {
            matrix_name: "ExampleMatrix".to_string(),
            num_samples: 100,
            millis_between_samples: Millis::new(500),
            worker_timeout_seconds: Seconds::new(3600),
        },
        producer_config: FluvioProducerConfig {
            batch_size: vec![16000],
            queue_size: vec![100],
            linger_millis: vec![Millis::new(10)],
            server_timeout_millis: vec![Millis::new(5000)],
            compression: vec![
                Compression::None,
                Compression::Gzip,
                Compression::Snappy,
                Compression::Lz4,
            ],
            isolation: vec![Isolation::ReadUncommitted, Isolation::ReadCommitted],
            delivery_semantic: vec![DeliverySemanticStrategy::AtMostOnce],
        },
        consumer_config: FluvioConsumerConfig {
            max_bytes: vec![64000],
            isolation: vec![Isolation::ReadUncommitted, Isolation::ReadCommitted],
        },
        topic_config: FluvioTopicConfig {
            num_partitions: vec![1],
        },
        load_config: BenchmarkLoadConfig {
            num_records_per_producer_worker_per_batch: vec![1000],
            record_key_allocation_strategy: vec![
                RecordKeyAllocationStrategy::NoKey,
                RecordKeyAllocationStrategy::AllShareSameKey,
                RecordKeyAllocationStrategy::ProducerWorkerUniqueKey,
                RecordKeyAllocationStrategy::RoundRobinKey(8),
                RecordKeyAllocationStrategy::RandomKey,
            ],
            num_concurrent_producer_workers: vec![1],
            num_concurrent_consumers_per_partition: vec![1],
            record_size: vec![1000],
        },
    };
    println!("{}", serde_yaml::to_string(&example_config).unwrap());
}

fn test_configs() -> Vec<BenchmarkMatrix> {
    let mut compression = BenchmarkMatrix::new("Test Compression");
    compression.producer_config.compression = vec![
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
    ];
    let mut record_key = BenchmarkMatrix::new("Test RecordKey");
    record_key.load_config.record_key_allocation_strategy = vec![
        RecordKeyAllocationStrategy::NoKey,
        RecordKeyAllocationStrategy::AllShareSameKey,
        RecordKeyAllocationStrategy::ProducerWorkerUniqueKey,
        RecordKeyAllocationStrategy::RoundRobinKey(8),
        RecordKeyAllocationStrategy::RandomKey,
    ];

    let mut concurrent = BenchmarkMatrix::new("Test concurrent producers and consumers");
    concurrent.load_config.num_concurrent_producer_workers = vec![3];
    concurrent
        .load_config
        .num_concurrent_consumers_per_partition = vec![3];

    let mut isolation = BenchmarkMatrix::new("Test producer and consumer isolation");
    isolation.producer_config.isolation =
        vec![Isolation::ReadUncommitted, Isolation::ReadCommitted];
    isolation.consumer_config.isolation =
        vec![Isolation::ReadUncommitted, Isolation::ReadCommitted];

    let mut delivery_semantic = BenchmarkMatrix::new("Test DeliverySemantic");
    delivery_semantic.producer_config.delivery_semantic = vec![
        DeliverySemanticStrategy::AtMostOnce,
        DeliverySemanticStrategy::AtLeastOnce(AtLeastOnceStrategy::Exponential),
        DeliverySemanticStrategy::AtLeastOnce(AtLeastOnceStrategy::Fixed),
        DeliverySemanticStrategy::AtLeastOnce(AtLeastOnceStrategy::Fibonacci),
    ];
    vec![
        compression,
        record_key,
        concurrent,
        isolation,
        delivery_semantic,
    ]
}

fn default_configs() -> Vec<BenchmarkMatrix> {
    vec![BenchmarkMatrix {
        shared_config: SharedConfig {
            matrix_name: "Fluvio Default Benchmark".to_string(),
            num_samples: 100,
            millis_between_samples: Millis::new(250),
            worker_timeout_seconds: Seconds::new(3000),
        },
        producer_config: FluvioProducerConfig {
            batch_size: vec![16000],
            queue_size: vec![100],
            linger_millis: vec![Millis::new(10)],
            server_timeout_millis: vec![Millis::new(5000)],
            compression: vec![Compression::None],
            isolation: vec![Isolation::ReadUncommitted],
            delivery_semantic: vec![DeliverySemanticStrategy::AtLeastOnce(
                AtLeastOnceStrategy::Exponential,
            )],
        },
        consumer_config: FluvioConsumerConfig {
            max_bytes: vec![64000],
            isolation: vec![Isolation::ReadUncommitted],
        },
        topic_config: FluvioTopicConfig {
            num_partitions: vec![1],
        },
        load_config: BenchmarkLoadConfig {
            num_records_per_producer_worker_per_batch: vec![100, 1000, 10000],
            record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
            num_concurrent_producer_workers: vec![1],
            num_concurrent_consumers_per_partition: vec![1],
            record_size: vec![1000],
        },
    }]
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to a config file to run. If not found, looks for config files in crates/fluvio-benchmark/benches/
    #[arg(short, long, exclusive(true))]
    config: Option<String>,

    /// Print out an example config file and then exit
    ///
    /// This file contains examples of all of the various configuration options
    #[arg(short, long, exclusive(true))]
    example_config: bool,

    /// Run a suite of tests to ensure fluvio is behaving as expected
    #[arg(short, long, exclusive(true))]
    test_cluster: bool,
}
