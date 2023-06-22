use std::time::Duration;

use fluvio_types::{ReplicationFactor, TopicName, PartitionCount, IgnoreRackAssignment};

use crate::topic::{
    ReplicaSpec, TopicReplicaParam, SegmentBasedPolicy, CleanupPolicy, TopicStorageConfig,
};

use super::{TopicSpec, PartitionMap, CompressionAlgorithm};

const DEFAULT_PARTITION_COUNT: PartitionCount = 1;
const DEFAULT_REPLICATION_FACTOR: ReplicationFactor = 1;
const DEFAULT_IGNORE_RACK_ASSIGMENT: IgnoreRackAssignment = false;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct TopicConfig {
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub version: Option<String>,

    pub meta: MetaConfig,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub partition: Option<PartitionConfig>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub retention: Option<RetentionConfig>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub compression: Option<CompressionConfig>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MetaConfig {
    pub name: TopicName,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionConfig {
    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub count: Option<PartitionCount>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub max_size: Option<bytesize::ByteSize>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub replication: Option<ReplicationFactor>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub ignore_rack_assignment: Option<IgnoreRackAssignment>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub maps: Option<Vec<PartitionMap>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RetentionConfig {
    #[cfg_attr(
        feature = "use_serde",
        serde(skip_serializing_if = "Option::is_none", with = "humantime_serde")
    )]
    pub time: Option<Duration>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub segment_size: Option<bytesize::ByteSize>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompressionConfig {
    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: CompressionAlgorithm,
}

impl TopicConfig {
    #[cfg(feature = "use_serde")]
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_reader(std::fs::File::open(path)?)?)
    }
}

#[cfg(feature = "use_serde")]
impl std::str::FromStr for TopicConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_yaml::from_str(s)?)
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            partition: Some(PartitionConfig {
                count: Some(DEFAULT_PARTITION_COUNT),
                replication: Some(DEFAULT_REPLICATION_FACTOR),
                ..Default::default()
            }),
            version: Default::default(),
            meta: Default::default(),
            retention: Default::default(),
            compression: Default::default(),
        }
    }
}

impl From<TopicConfig> for TopicSpec {
    fn from(config: TopicConfig) -> Self {
        let segment_size = config
            .retention
            .as_ref()
            .and_then(|r| r.segment_size)
            .map(|s| s.as_u64() as u32);
        let max_partition_size = config
            .partition
            .as_ref()
            .and_then(|r| r.max_size)
            .map(|s| s.as_u64());

        let replica_spec = match config.partition {
            Some(partition) => match partition.maps {
                Some(maps) => ReplicaSpec::Assigned(maps.into()),
                None => ReplicaSpec::Computed(TopicReplicaParam {
                    partitions: partition.count.unwrap_or(DEFAULT_PARTITION_COUNT),
                    replication_factor: partition.replication.unwrap_or(DEFAULT_REPLICATION_FACTOR),
                    ignore_rack_assignment: partition
                        .ignore_rack_assignment
                        .unwrap_or(DEFAULT_IGNORE_RACK_ASSIGMENT),
                }),
            },
            None => ReplicaSpec::Computed(TopicReplicaParam {
                partitions: DEFAULT_PARTITION_COUNT,
                replication_factor: DEFAULT_REPLICATION_FACTOR,
                ignore_rack_assignment: DEFAULT_IGNORE_RACK_ASSIGMENT,
            }),
        };
        let mut topic_spec: TopicSpec = replica_spec.into();
        if let Some(retention_time) = config.retention.and_then(|r| r.time) {
            topic_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: retention_time.as_secs() as u32,
            }));
        };
        if let Some(compression) = config.compression {
            topic_spec.set_compression_type(compression.type_);
        }

        if segment_size.is_some() || max_partition_size.is_some() {
            topic_spec.set_storage(TopicStorageConfig {
                segment_size,
                max_partition_size,
            });
        }

        topic_spec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_topic_config_ser_de() {
        //given
        let input = r#"version: 0.1.0
meta:
  name: test_topic
partition:
  count: 3
  max_size: 1.0 KB
  replication: 2
  ignore_rack_assignment: true
  maps:
  - id: 1
    replicas:
    - 1
    - 2
retention:
  time: 2m
  segment_size: 2.0 KB
compression:
  type: Lz4
"#;

        //when
        use std::str::FromStr;

        let deser = TopicConfig::from_str(input).expect("deserialized");
        let ser = serde_yaml::to_string(&deser).expect("serialized");

        //then
        assert_eq!(input, ser);
        assert_eq!(deser, test_config());
    }

    #[test]
    fn test_default_config_to_spec() {
        //given
        let config = TopicConfig::default();

        //when
        let spec: TopicSpec = config.into();

        //then
        assert_eq!(
            spec,
            TopicSpec::new_computed(
                DEFAULT_PARTITION_COUNT,
                DEFAULT_REPLICATION_FACTOR,
                Some(DEFAULT_IGNORE_RACK_ASSIGMENT)
            )
        );
    }

    #[test]
    fn test_full_config_to_spec() {
        //given
        let config = test_config();

        //when
        let spec: TopicSpec = config.into();

        //then
        let mut test_spec = TopicSpec::new_assigned(vec![PartitionMap {
            id: 1,
            replicas: vec![1, 2],
        }]);
        test_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
            time_in_seconds: 120,
        }));
        test_spec.set_compression_type(CompressionAlgorithm::Lz4);
        test_spec.set_storage(TopicStorageConfig {
            segment_size: Some(2000),
            max_partition_size: Some(1000),
        });

        assert_eq!(spec, test_spec);
    }

    fn test_config() -> TopicConfig {
        TopicConfig {
            version: Some("0.1.0".to_string()),
            meta: MetaConfig {
                name: "test_topic".to_string(),
            },
            partition: Some(PartitionConfig {
                count: Some(3),
                max_size: Some(bytesize::ByteSize(1000)),
                replication: Some(2),
                ignore_rack_assignment: Some(true),
                maps: Some(vec![PartitionMap {
                    id: 1,
                    replicas: vec![1, 2],
                }]),
            }),
            retention: Some(RetentionConfig {
                time: Some(Duration::from_secs(120)),
                segment_size: Some(bytesize::ByteSize(2000)),
            }),
            compression: Some(CompressionConfig {
                type_: CompressionAlgorithm::Lz4,
            }),
        }
    }
}
