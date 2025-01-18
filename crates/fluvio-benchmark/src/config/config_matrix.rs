use std::time::Duration;

use fluvio::Compression;
use serde::{Deserialize, Serialize};
use bytesize::ByteSize;

use crate::config::{BenchmarkConfig, RecordKeyAllocationStrategy};

use super::{cross::CrossIterate, default_topic_name, ProducerConfigBuilder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Matrix {
    pub producer_config: Option<ProducerMatrixConfig>,
    pub consumer_config: Option<ConsumerMatrixConfig>,
    pub shared_config: SharedMatrixConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerMatrixConfig {
    pub batch_size: Vec<ByteSize>,
    pub queue_size: Vec<u64>,
    pub max_request_size: Vec<ByteSize>,
    pub linger: Vec<Duration>,
    pub server_timeout: Vec<Duration>,
    pub compression: Vec<Compression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMatrixConfig {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SharedMatrixConfig {
    pub num_samples: Vec<usize>,
    pub time_between_samples: Vec<Duration>,
    pub worker_timeout: Vec<Duration>,
    pub topic_config: FluvioTopicMatrixConfig,
    pub load_config: BenchmarkLoadMatrixConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkLoadMatrixConfig {
    pub record_key_allocation_strategy: Vec<RecordKeyAllocationStrategy>,
    pub num_producers: Vec<u64>,
    pub num_records: Vec<u64>,
    pub record_size: Vec<ByteSize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FluvioTopicMatrixConfig {
    pub partitions: Vec<u32>,
    pub replicas: Vec<u32>,
    pub topic_name: Vec<String>,
    pub delete_topic: Vec<bool>,
    pub ignore_rack: Vec<bool>,
}

impl Matrix {
    pub fn generate_configs(&self) -> Vec<BenchmarkConfig> {
        let builder: Vec<ProducerConfigBuilder> = vec![ProducerConfigBuilder::default()];

        if let Some(producer_config) = &self.producer_config {
            let producer_config = builder
                .cross_iterate(&producer_config.batch_size, |v, b| {
                    b.batch_size(v);
                })
                .cross_iterate(&producer_config.queue_size, |v, b| {
                    b.queue_size(v);
                })
                .cross_iterate(&producer_config.max_request_size, |v, b| {
                    b.max_request_size(v);
                })
                .cross_iterate(&producer_config.linger, |v, b| {
                    b.linger(v);
                })
                .cross_iterate(&producer_config.server_timeout, |v, b| {
                    b.server_timeout(v);
                })
                .cross_iterate(&producer_config.compression, |v, b| {
                    b.compression(v);
                })
                .cross_iterate(&self.shared_config.num_samples, |v, b| {
                    b.num_samples(v);
                })
                .cross_iterate(&self.shared_config.time_between_samples, |v, b| {
                    b.time_between_samples(v);
                })
                .cross_iterate(&self.shared_config.worker_timeout, |v, b| {
                    b.worker_timeout(v);
                })
                .cross_iterate(&self.shared_config.topic_config.partitions, |v, b| {
                    b.partitions(v);
                })
                .cross_iterate(&self.shared_config.topic_config.replicas, |v, b| {
                    b.replicas(v);
                })
                .cross_iterate(&self.shared_config.topic_config.topic_name, |v, b| {
                    b.topic_name(v);
                })
                .cross_iterate(&self.shared_config.topic_config.delete_topic, |v, b| {
                    b.delete_topic(v);
                })
                .cross_iterate(&self.shared_config.topic_config.ignore_rack, |v, b| {
                    b.ignore_rack(v);
                })
                .cross_iterate(
                    &self
                        .shared_config
                        .load_config
                        .record_key_allocation_strategy,
                    |v, b| {
                        b.record_key_allocation_strategy(v);
                    },
                )
                .cross_iterate(&self.shared_config.load_config.num_producers, |v, b| {
                    b.num_producers(v);
                })
                .cross_iterate(&self.shared_config.load_config.num_records, |v, b| {
                    b.num_records(v);
                })
                .cross_iterate(&self.shared_config.load_config.record_size, |v, b| {
                    b.record_size(v);
                })
                .build();

            return producer_config
                .into_iter()
                .map(BenchmarkConfig::Producer)
                .collect();
        }

        if let Some(_consumer) = &self.consumer_config {
            todo!("Consumer config not implemented");
        }

        panic!("No producer or consumer config provided");
    }
}

pub fn default_config() -> Matrix {
    Matrix {
        producer_config: Some(ProducerMatrixConfig {
            batch_size: vec![ByteSize::kib(16), ByteSize::mib(1)],
            queue_size: vec![100],
            max_request_size: vec![ByteSize::mb(32)],
            linger: vec![Duration::from_millis(0)],
            server_timeout: vec![Duration::from_secs(600)],
            compression: vec![Compression::None],
        }),
        consumer_config: None,
        shared_config: SharedMatrixConfig {
            num_samples: vec![2],
            time_between_samples: vec![Duration::from_secs(1)],
            worker_timeout: vec![Duration::from_secs(60)],
            topic_config: FluvioTopicMatrixConfig {
                partitions: vec![1],
                replicas: vec![1],
                topic_name: vec![default_topic_name()],
                delete_topic: vec![true],
                ignore_rack: vec![true],
            },
            load_config: BenchmarkLoadMatrixConfig {
                record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
                num_producers: vec![1],
                num_records: vec![100, 10_000, 100_000],
                record_size: vec![ByteSize::kib(5)],
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let matrix = default_config();
        let configs = matrix.generate_configs();

        assert_eq!(configs.len(), 6);
    }
}
