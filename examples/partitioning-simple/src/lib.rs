use fluvio::{dataplane::types::PartitionId, Partitioner, PartitionerConfig};

/// A simple partitioning that treat key as string and try to map it to partition from letter count
/// aa -> 1, bbb -> 2, cccc -> 3
/// If key is not provided, it will always return 0 or key is not properly string
/// If length of key is greater than partition count, it will wrap around
/// String is assumed to be utf-8 format
/// To configure Fluvio producer
///
/// ```
/// async fn example() {
///     use std::sync::Arc;
///     use fluvio::{Partitioner,PartitionerConfig};
///     use partitioning_simple::AlphabetPartitioning;
///
///     let config = fluvio::TopicProducerConfigBuilder::default()
///         .partitioner(Arc::new(AlphabetPartitioning::new()))
///         .build()
///         .expect("Failed to create a config");
///     let fluvio_instance = fluvio::Fluvio::connect().await.expect("");
///     let producer = fluvio_instance.topic_producer_with_config("my-fluvio-topic", config).await.expect("Failed to create a producer");
///}
/// ```
pub struct AlphabetPartitioning {}

impl AlphabetPartitioning {
    pub fn new() -> Self {
        Self {}
    }
}

impl Partitioner for AlphabetPartitioning {
    fn partition(
        &self,
        config: &PartitionerConfig,
        maybe_key: Option<&[u8]>,
        _value: &[u8],
    ) -> PartitionId {
        match maybe_key {
            Some(key) => match std::str::from_utf8(key) {
                Ok(key_str) => {
                    let count = key_str.chars().count() as u32;
                    let partition_id = count % config.partition_count() as u32;
                    partition_id
                }
                Err(_) => 0,
            },
            None => 0,
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test() {
        let partitioner = AlphabetPartitioning::new();
        assert_eq!(
            partitioner.partition(&PartitionerConfig { partition_count: 3 }, Some(b"aa"), &[]),
            2
        );
        assert_eq!(
            partitioner.partition(&PartitionerConfig { partition_count: 3 }, Some(b"a"), &[]),
            1
        );
        assert_eq!(
            partitioner.partition(&PartitionerConfig { partition_count: 3 }, None, &[]),
            0
        );
        assert_eq!(
            partitioner.partition(
                &PartitionerConfig { partition_count: 3 },
                Some(b"abcdefg"),
                &[]
            ),
            1
        ); // 7 % 3 = 1
    }
}
