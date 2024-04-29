use std::time::Duration;

use derive_builder::Builder;
use fluvio_types::{ReplicationFactor, TopicName, PartitionCount, IgnoreRackAssignment};

use crate::topic::{
    ReplicaSpec, TopicReplicaParam, SegmentBasedPolicy, CleanupPolicy, TopicStorageConfig,
};

use super::{TopicSpec, PartitionMap, CompressionAlgorithm, deduplication::Deduplication};

const DEFAULT_PARTITION_COUNT: PartitionCount = 1;
const DEFAULT_REPLICATION_FACTOR: ReplicationFactor = 1;
const DEFAULT_IGNORE_RACK_ASSIGMENT: IgnoreRackAssignment = false;
const DEFAULT_VERSION: &str = "0.1.0";

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct TopicConfig {
    #[cfg_attr(feature = "use_serde", serde(default = "default_version"))]
    #[builder(default = "default_version()")]
    pub version: String,

    pub meta: MetaConfig,

    #[builder(default)]
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub partition: PartitionConfig,

    #[builder(default)]
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub retention: RetentionConfig,

    #[builder(default)]
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub compression: CompressionConfig,

    #[builder(default)]
    #[cfg_attr(
        feature = "use_serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub deduplication: Option<Deduplication>,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct MetaConfig {
    pub name: TopicName,
}

#[derive(Debug, Builder, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
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

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct RetentionConfig {
    #[cfg_attr(
        feature = "use_serde",
        serde(skip_serializing_if = "Option::is_none", with = "humantime_serde")
    )]
    pub time: Option<Duration>,

    #[cfg_attr(feature = "use_serde", serde(skip_serializing_if = "Option::is_none"))]
    pub segment_size: Option<bytesize::ByteSize>,
}

#[derive(Debug, Default, Builder, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
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

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            count: Some(DEFAULT_PARTITION_COUNT),
            replication: Some(DEFAULT_REPLICATION_FACTOR),
            ignore_rack_assignment: Some(DEFAULT_IGNORE_RACK_ASSIGMENT),
            max_size: Default::default(),
            maps: Default::default(),
        }
    }
}

impl From<TopicConfig> for TopicSpec {
    fn from(config: TopicConfig) -> Self {
        let segment_size = config.retention.segment_size.map(|s| s.as_u64() as u32);
        let max_partition_size = config.partition.max_size.map(|s| s.as_u64());

        let replica_spec = match config.partition.maps {
            Some(maps) => ReplicaSpec::Assigned(maps.into()),
            None => ReplicaSpec::Computed(TopicReplicaParam {
                partitions: config.partition.count.unwrap_or(DEFAULT_PARTITION_COUNT),
                replication_factor: config
                    .partition
                    .replication
                    .unwrap_or(DEFAULT_REPLICATION_FACTOR),
                ignore_rack_assignment: config
                    .partition
                    .ignore_rack_assignment
                    .unwrap_or(DEFAULT_IGNORE_RACK_ASSIGMENT),
            }),
        };
        let mut topic_spec: TopicSpec = replica_spec.into();
        if let Some(retention_time) = config.retention.time {
            topic_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: retention_time.as_secs() as u32,
            }));
        };

        topic_spec.set_compression_type(config.compression.type_);
        topic_spec.set_deduplication(config.deduplication);

        if segment_size.is_some() || max_partition_size.is_some() {
            topic_spec.set_storage(TopicStorageConfig {
                segment_size,
                max_partition_size,
            });
        }

        topic_spec
    }
}

fn default_version() -> String {
    DEFAULT_VERSION.to_string()
}

#[cfg(test)]
mod tests {
    use crate::topic::deduplication::{Bounds, Filter, Transform};

    use super::*;

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_topic_config_ser_de() {
        //given
        let input = r#"version: 0.1.1
meta:
  name: test_topic
partition:
  count: 3
  max-size: 1.0 KB
  replication: 2
  ignore-rack-assignment: true
  maps:
  - id: 1
    replicas:
    - 1
    - 2
retention:
  time: 2m
  segment-size: 2.0 KB
compression:
  type: Lz4
deduplication:
  bounds:
    count: 100
    age: 1m
  filter:
    transform:
      uses: infinyon/dedup-filter@0.1.0
"#;

        //when
        use std::str::FromStr;

        let deser = TopicConfig::from_str(input).expect("deserialized");
        let ser = serde_yaml::to_string(&deser).expect("serialized");

        //then
        assert_eq!(input, ser);
        assert_eq!(deser, test_config());
    }

    #[cfg(feature = "use_serde")]
    #[test]
    fn test_minimal_topic_config_ser_de() {
        //given
        let input = r#"meta:
  name: test_topic
"#;

        //when
        use std::str::FromStr;

        let deser = TopicConfig::from_str(input).expect("deserialized");
        let ser = serde_yaml::to_string(&deser).expect("serialized");

        //then
        assert_eq!(deser.version, DEFAULT_VERSION);
        assert_eq!(deser.partition.count, Some(DEFAULT_PARTITION_COUNT));
        assert_eq!(
            deser.partition.replication,
            Some(DEFAULT_REPLICATION_FACTOR)
        );
        assert_eq!(
            deser.partition.ignore_rack_assignment,
            Some(DEFAULT_IGNORE_RACK_ASSIGMENT)
        );
        assert_eq!(
            ser,
            r#"version: 0.1.0
meta:
  name: test_topic
partition:
  count: 1
  replication: 1
  ignore-rack-assignment: false
retention: {}
compression:
  type: Any
"#
        );
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
            ..Default::default()
        }]);
        test_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
            time_in_seconds: 120,
        }));
        test_spec.set_compression_type(CompressionAlgorithm::Lz4);
        test_spec.set_storage(TopicStorageConfig {
            segment_size: Some(2000),
            max_partition_size: Some(1000),
        });
        test_spec.set_deduplication(Some(test_deduplication()));

        assert_eq!(spec, test_spec);
    }

    fn test_config() -> TopicConfig {
        TopicConfig {
            version: "0.1.1".to_string(),
            meta: MetaConfig {
                name: "test_topic".to_string(),
            },
            partition: PartitionConfig {
                count: Some(3),
                max_size: Some(bytesize::ByteSize(1000)),
                replication: Some(2),
                ignore_rack_assignment: Some(true),
                maps: Some(vec![PartitionMap {
                    id: 1,
                    replicas: vec![1, 2],
                    ..Default::default()
                }]),
            },
            retention: RetentionConfig {
                time: Some(Duration::from_secs(120)),
                segment_size: Some(bytesize::ByteSize(2000)),
            },
            compression: CompressionConfig {
                type_: CompressionAlgorithm::Lz4,
            },
            deduplication: Some(test_deduplication()),
        }
    }

    fn test_deduplication() -> Deduplication {
        Deduplication {
            bounds: Bounds {
                count: 100,
                age: Some(Duration::from_secs(60)),
            },
            filter: Filter {
                transform: Transform {
                    uses: "infinyon/dedup-filter@0.1.0".to_string(),
                    with: Default::default(),
                },
            },
        }
    }
}
