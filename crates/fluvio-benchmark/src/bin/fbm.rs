use clap::{arg, Parser};
use anyhow::Result;

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
};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_logger();
    let args = Args::parse();

    if args.example_config {
        print_example_config();
        return Ok(());
    }

    // TODO accept directory of files.
    let mut matrices = if args.test_cluster {
        test_configs()
    } else {
        match args.config {
            Some(path) => get_config_from_file(&path),
            None => default_configs(),
        }
    };

    for matrix in &mut matrices {
        if let Some(producer_batch_size) = args.producer_batch_size {
            matrix.producer_config.batch_size = vec![producer_batch_size];
        }
        if let Some(record_size) = args.record_size {
            matrix.load_config.record_size = vec![record_size];
        }
    }

    run_block_on(run_benchmark(matrices))
}

async fn run_benchmark(matrices: Vec<BenchmarkMatrix>) -> Result<()> {
    // let previous = load_previous_stats();

    //println!("# Fluvio Benchmark Results");
    for matrix in matrices {
        // println!(" Matrix: {}", matrix.shared_config.matrix_name);
        println!("{:#?}", matrix);
        for (i, config) in matrix.into_iter().enumerate() {
            BenchmarkDriver::run_benchmark(config.clone()).await?;
            println!("### {}: Iteration {:3.0}", config.matrix_name, i);
            println!("{}", config.to_markdown());
            println!();
        }
    }

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
            batch_size: vec![1048576],
            queue_size: vec![100],
            linger_millis: vec![Millis::new(100)],
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
            record_size: vec![5000],
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
            batch_size: vec![1000000],
            queue_size: vec![10],
            linger_millis: vec![Millis::new(0)],
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
            num_records_per_producer_worker_per_batch: vec![100],
            record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
            num_concurrent_producer_workers: vec![1],
            num_concurrent_consumers_per_partition: vec![1],
            record_size: vec![5000],
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

    #[arg(long)]
    producer_batch_size: Option<u64>,

    #[arg(long)]
    record_size: Option<u64>,
}
