use std::time::Duration;
use fluvio::Compression;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs::File;

use crate::benchmark_config::benchmark_settings::generate_new_topic_name;

use super::benchmark_settings::BenchmarkSettings;
/// Key used by AllShareSameKey
pub const SHARED_KEY: &'static str = "SHARED_KEY";

/// DEFAULT CONFIG DIR
pub const DEFAULT_CONFIG_DIR: &'static str = "crates/fluvio-benchmark/benches";

/// A BenchmarkMatrix contains a collection of settings and dimensions.
/// Iterating over a BenchmarkMatrix produces a BenchmarkSettings for every possible combination of values in the matrix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkMatrix {
    /// Each sample is a collection of batches that all run on the same topic.
    pub matrix_name: String,
    pub num_samples: u64,
    pub num_batches_per_sample: u64,
    pub seconds_between_batches: u64,
    pub worker_timeout_seconds: u64,
    pub num_records_per_producer_worker_per_batch: Vec<u64>,
    pub producer_batch_size: Vec<u64>,
    pub producer_queue_size: Vec<u64>,
    pub producer_linger_millis: Vec<u64>,
    pub producer_server_timeout_millis: Vec<u64>,
    pub producer_compression: Vec<Compression>,
    pub record_key_allocation_strategy: Vec<RecordKeyAllocationStrategy>,
    // TODO
    // pub producer_isolation:...,
    // TODO
    // pub producer_delivery_semantic,
    pub consumer_max_bytes: Vec<u64>,
    // TODO
    // pub consumer_isolation:...,
    pub num_concurrent_producer_workers: Vec<u64>,
    /// Total number of concurrent consumers equals num_concurrent_consumers_per_partition * num_partitions
    pub num_concurrent_consumers_per_partition: Vec<u64>,
    pub num_partitions: Vec<u64>,
    // TODO
    // pub num_replicas: Vec<u64>,
    pub record_size_strategy: Vec<RecordSizeStrategy>,
    // TODO
    // pub use_smart_module: Vec<bool>,
    // TODO
    // IgnoreRac
}

impl IntoIterator for BenchmarkMatrix {
    type Item = BenchmarkSettings;

    type IntoIter = <Vec<BenchmarkSettings> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.generate_settings().into_iter()
    }
}

impl BenchmarkMatrix {
    // Impl note: This does allocate for all of the benchmark settings at once, however it made for simpler code
    // and as there is a very low practical limit for the number of benchmarks that can be run in a reasonable time period, its not an issue that it alloates.
    // TODO split into smaller chunks
    fn generate_settings(&self) -> Vec<BenchmarkSettings> {
        let mut settings = Vec::new();
        for num_records_per_producer_worker_per_batch in
            self.num_records_per_producer_worker_per_batch.iter()
        {
            for producer_batch_size in self.producer_batch_size.iter() {
                for producer_queue_size in self.producer_queue_size.iter() {
                    for producer_linger in self
                        .producer_linger_millis
                        .iter()
                        .map(|i| Duration::from_millis(*i))
                    {
                        for producer_server_timeout in self
                            .producer_server_timeout_millis
                            .iter()
                            .map(|i| Duration::from_millis(*i))
                        {
                            for producer_compression in self.producer_compression.iter() {
                                for record_key_allocation_strategy in
                                    self.record_key_allocation_strategy.iter()
                                {
                                    for consumer_max_bytes in self.consumer_max_bytes.iter() {
                                        for num_concurrent_producer_workers in
                                            self.num_concurrent_producer_workers.iter()
                                        {
                                            for num_concurrent_consumers_per_partition in
                                                self.num_concurrent_consumers_per_partition.iter()
                                            {
                                                for num_partitions in self.num_partitions.iter() {
                                                    for record_size_strategy in
                                                        self.record_size_strategy.iter()
                                                    {
                                                        settings.push(BenchmarkSettings {
                                                    topic_name:generate_new_topic_name(),
                                                    num_samples: self.num_samples,
                                                    num_batches_per_sample: self.num_batches_per_sample,
                                                    duration_between_batches: Duration::from_secs(self.seconds_between_batches),
                                                    worker_timeout: Duration::from_secs(self.worker_timeout_seconds),
                                                    num_records_per_producer_worker_per_batch:
                                                        *num_records_per_producer_worker_per_batch,
                                                    producer_batch_size: *producer_batch_size,
                                                    producer_queue_size: *producer_queue_size,
                                                    producer_linger,
                                                    producer_server_timeout,
                                                    producer_compression: *producer_compression,
                                                    record_key_allocation_strategy: *record_key_allocation_strategy,
                                                    consumer_max_bytes: *consumer_max_bytes,
                                                    num_concurrent_producer_workers:
                                                        *num_concurrent_producer_workers,
                                                    num_concurrent_consumers_per_partition:
                                                        *num_concurrent_consumers_per_partition,
                                                    num_partitions: *num_partitions,
                                                    record_size_strategy: *record_size_strategy,
                                                })
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        info!("Iterating over {} test setting(s)", settings.len());
        settings
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum RecordSizeStrategy {
    Fixed(u64),
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum RecordKeyAllocationStrategy {
    /// RecordKey::NULL
    NoKey,
    /// All producer workers will use the same key
    AllShareSameKey,

    /// Each producer will use the same key for each of their records
    ProducerWorkerUniqueKey,

    /// Each producer will round robin from 0..N for each record produced
    RoundRobinKey(u64),

    RandomKey,
}

pub fn get_default_config() -> Vec<BenchmarkMatrix> {
    walkdir::WalkDir::new(DEFAULT_CONFIG_DIR)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            if e.path().extension().is_some() {
                Some(e)
            } else {
                None
            }
        })
        .map(|e| {
            let file = File::open(e.path()).unwrap();
            serde_yaml::from_reader::<_, BenchmarkMatrix>(file).unwrap()
        })
        .collect()
}

pub fn get_config_from_file(path: &str) -> Vec<BenchmarkMatrix> {
    let file = File::open(path).unwrap();
    vec![serde_yaml::from_reader::<_, BenchmarkMatrix>(file).unwrap()]
}

#[cfg(test)]
mod tests {

    use std::{path::Path, fs::File};

    use super::BenchmarkMatrix;

    fn test_config(path: &Path) -> bool {
        let file = File::open(path).expect("Unable to open file");
        match serde_yaml::from_reader::<_, BenchmarkMatrix>(file) {
            Ok(_) => true,
            Err(e) => {
                println!("failed to parse configuration at {}: {}", path.display(), e);
                false
            }
        }
    }

    #[test]
    fn load_configs() {
        // make sure all the configs in benches can be parsed correctly
        let mut success = true;
        for file in walkdir::WalkDir::new("benches")
            .into_iter()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                if e.path().extension().is_some() {
                    Some(e)
                } else {
                    None
                }
            })
        {
            success = success && test_config(file.path());
        }
        assert!(success, "one or more configuration files failed to parse");
    }
}
