//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!
use std::path::PathBuf;
use std::time::Duration;

use tracing::debug;
use clap::Parser;
use humantime::parse_duration;
use anyhow::Result;

use fluvio_types::PartitionCount;
use fluvio_types::ReplicationFactor;
use fluvio::metadata::topic::CleanupPolicy;
use fluvio::metadata::topic::ReplicaSpec;
use fluvio::metadata::topic::SegmentBasedPolicy;
use fluvio::metadata::topic::TopicStorageConfig;
use fluvio::metadata::topic::CompressionAlgorithm;

use fluvio_controlplane_metadata::topic::config::TopicConfig;
use fluvio_sc_schema::shared::validate_resource_name;
use fluvio_sc_schema::mirror::MirrorSpec;
use fluvio_sc_schema::topic::HomeMirrorConfig;
use fluvio_sc_schema::topic::MirrorConfig;

use fluvio::Fluvio;
use fluvio::FluvioAdmin;
use fluvio::metadata::topic::TopicSpec;
use crate::CliError;

#[derive(Debug, Parser)]
pub struct CreateTopicOpt {
    /// The name of the Topic to create
    #[arg(value_name = "name", group = "config-arg")]
    topic: Option<String>,

    /// The number of Partitions to give the Topic
    ///
    /// Partitions are a way to divide the total traffic of a single Topic into
    /// separate streams which may be processed independently. Data sent to different
    /// partitions may be processed by separate SPUs on different computers. By
    /// dividing the load of a Topic evenly among partitions, you can increase the
    /// total throughput of the Topic.
    #[arg(
        short = 'p',
        long = "partitions",
        value_name = "partitions",
        default_value = "1",
        group = "config-arg"
    )]
    partitions: PartitionCount,

    /// The number of full replicas of the Topic to keep
    ///
    /// The Replication Factor describes how many copies of
    /// the Topic's data should be kept. If the Topic has a
    /// replication factor of 2, then all of the data in the
    /// Topic must be fully stored on at least 2 separate SPUs.
    ///
    /// This applies to each Partition in the Topic. If we have
    /// 3 partitions and a replication factor of 2, then all 3
    /// of the partitions must exist on at least 2 SPUs.
    #[arg(
        short = 'r',
        long = "replication",
        value_name = "integer",
        default_value = "1",
        group = "config-arg"
    )]
    replication: i16,

    /// Ignore racks while computing replica assignment
    #[arg(
        short = 'i',
        long = "ignore-rack-assignment",
        conflicts_with = "replica_assignment",
        group = "config-arg"
    )]
    ignore_rack_assignment: bool,

    /// Replica assignment file
    #[arg(
        short = 'f',
        long = "replica-assignment",
        value_name = "file.json",
        conflicts_with = "partitions",
        conflicts_with = "replication",
        conflicts_with = "mirror_apply",
        conflicts_with = "mirror",
        group = "config-arg"
    )]
    replica_assignment: Option<PathBuf>,

    /// Mirror apply file
    #[arg(
        short = 'm',
        long = "mirror-apply",
        value_name = "file.json",
        conflicts_with = "partitions",
        conflicts_with = "replication",
        conflicts_with = "replica_assignment",
        conflicts_with = "mirror",
        group = "config-arg"
    )]
    mirror_apply: Option<PathBuf>,

    /// Flag for a mirror topic
    #[arg(
        long = "mirror",
        conflicts_with = "partitions",
        conflicts_with = "mirror_apply",
        conflicts_with = "replication",
        conflicts_with = "replica_assignment",
        group = "config-arg"
    )]
    mirror: bool,

    /// Validates configuration, does not provision
    #[arg(short = 'd', long)]
    dry_run: bool,

    #[clap(flatten)]
    setting: TopicConfigOpt,

    /// Path to topic configuration file
    ///
    /// All configuration parameters can be specified either as command-line arguments
    /// or inside the topic configuration file in YAML format.
    #[arg(short, long, value_name = "PATH", conflicts_with = "config-arg")]
    config: Option<PathBuf>,

    /// signify that this topic can be mirror from home to edge
    #[arg(long)]
    home_to_remote: bool,
}

impl CreateTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let dry_run = self.dry_run;
        let admin = fluvio.admin().await;
        let (name, topic_spec) = self.construct(&admin).await?;
        validate(&name, &topic_spec)?;

        debug!("creating topic: {} spec: {:#?}", name, topic_spec);
        admin.create(name.clone(), dry_run, topic_spec).await?;
        println!("topic \"{name}\" created");

        Ok(())
    }

    async fn construct(self, admin: &FluvioAdmin) -> Result<(String, TopicSpec)> {
        if let Some(config_path) = self.config {
            let config = TopicConfig::from_file(config_path)?;
            return Ok((config.meta.name.clone(), config.into()));
        }

        let topic_name = self.topic.unwrap_or_default();

        use fluvio::metadata::topic::{PartitionMaps, TopicReplicaParam};
        use load::ReadFromJson;

        let replica_spec = if let Some(replica_assign_file) = &self.replica_assignment {
            ReplicaSpec::Assigned(PartitionMaps::read_from_json_file(
                replica_assign_file,
                &topic_name,
            )?)
        } else if let Some(mirror_assign_file) = &self.mirror_apply {
            let mut config = MirrorConfig::read_from_json_file(mirror_assign_file, &topic_name)?;

            config.set_home_to_remote(self.home_to_remote)?;

            let targets = match config {
                MirrorConfig::Home(ref c) => c
                    .partitions()
                    .iter()
                    .map(|p| p.remote_cluster.clone())
                    .collect::<Vec<String>>(),
                MirrorConfig::Remote(_) => {
                    return Err(
                        CliError::InvalidArg("Invalid mirror configuration".to_string()).into(),
                    )
                }
            };

            // validate if all mirrors are registered
            let mirrors = admin
                .all::<MirrorSpec>()
                .await?
                .iter()
                .map(|r| r.name.clone())
                .collect::<Vec<String>>();
            let not_registered_mirrors = targets
                .iter()
                .filter(|t| !mirrors.contains(t))
                .collect::<Vec<&String>>();

            if !not_registered_mirrors.is_empty() {
                return Err(CliError::InvalidArg(format!(
                    "Remote clusters not registered: {:?}",
                    not_registered_mirrors
                ))
                .into());
            }

            ReplicaSpec::Mirror(config)
        } else if self.mirror {
            let mut home_mirror = HomeMirrorConfig::from(vec![]);
            home_mirror.source = self.home_to_remote;
            let mirror_map = MirrorConfig::Home(home_mirror);
            ReplicaSpec::Mirror(mirror_map)
        } else {
            ReplicaSpec::Computed(TopicReplicaParam {
                partitions: self.partitions,
                replication_factor: self.replication as ReplicationFactor,
                ignore_rack_assignment: self.ignore_rack_assignment,
            })
        };

        let mut topic_spec: TopicSpec = replica_spec.into();
        if let Some(retention) = self.setting.retention_time {
            topic_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: retention.as_secs() as u32,
            }));
        }

        if let Some(compression_type) = self.setting.compression_type {
            topic_spec.set_compression_type(compression_type);
        }

        topic_spec.set_system(self.setting.system);

        if self.setting.segment_size.is_some() || self.setting.max_partition_size.is_some() {
            let mut storage = TopicStorageConfig::default();

            if let Some(segment_size) = self.setting.segment_size {
                storage.segment_size = Some(segment_size.as_u64() as u32);
            }

            if let Some(max_partition_size) = self.setting.max_partition_size {
                storage.max_partition_size = Some(max_partition_size.as_u64());
            }

            topic_spec.set_storage(storage);
        }

        Ok((topic_name, topic_spec))
    }
}

fn validate(name: &str, _spec: &TopicSpec) -> Result<()> {
    if name.trim().is_empty() {
        return Err(CliError::InvalidArg("Topic name is required".to_string()).into());
    }
    validate_resource_name(name)
        .map_err(|err| CliError::InvalidArg(format!("Invalid Topic name {}. {err}", name)))?;
    Ok(())
}

#[derive(Debug, Parser)]
#[group(id = "config-arg")]
pub struct TopicConfigOpt {
    /// Retention time (round to seconds)
    /// Ex: '1h', '2d 10s', '7 days' (default)
    #[arg(long, value_name = "time",value_parser=parse_duration)]
    retention_time: Option<Duration>,

    /// Segment size (by default measured in bytes)
    /// Ex: `2048`, '2 Ki', '10 MiB', `1 GB`
    #[arg(long, value_name = "bytes")]
    segment_size: Option<bytesize::ByteSize>,

    /// Compression configuration for topic
    #[arg(long, value_name = "compression")]
    compression_type: Option<CompressionAlgorithm>,

    /// Max partition size (by default measured in bytes)
    /// Ex: `2048`, '2 Ki', '10 MiB', `1 GB`
    #[arg(long, value_name = "bytes")]
    max_partition_size: Option<bytesize::ByteSize>,

    /// Flag to create a system topic
    /// System topics are for internal operations
    #[arg(long, short = 's', hide = true)]
    system: bool,
}

/// module to load partitions maps from file
mod load {

    use std::fs::read_to_string;
    use std::path::Path;

    use anyhow::{anyhow, Result};
    use fluvio::metadata::topic::{PartitionMaps, MirrorConfig};
    use fluvio_controlplane_metadata::topic::HomeMirrorConfig;

    pub(crate) trait ReadFromJson: Sized {
        /// Read and decode from json file
        fn read_from_json_file<T: AsRef<Path>>(path: T, topic: &str) -> Result<Self>;
    }

    impl ReadFromJson for PartitionMaps {
        fn read_from_json_file<T: AsRef<Path>>(path: T, _topic: &str) -> Result<Self> {
            let file_str: String = read_to_string(path)?;
            serde_json::from_str(&file_str)
                .map_err(|err| anyhow!("error reading replica assignment: {err}"))
        }
    }

    impl ReadFromJson for MirrorConfig {
        fn read_from_json_file<T: AsRef<Path>>(path: T, topic: &str) -> Result<Self> {
            let file_str: String = read_to_string(path)?;
            let clusters: Vec<String> = serde_json::from_str(&file_str)
                .map_err(|err| anyhow!("error reading mirror assignment: {err}"))?;
            let core_mirror = HomeMirrorConfig::from_simple(topic, clusters);
            Ok(MirrorConfig::Home(core_mirror))
        }
    }

    #[cfg(test)]
    mod test {

        use fluvio_controlplane_metadata::{
            topic::{PartitionMaps, MirrorConfig},
            partition::HomePartitionConfig,
        };

        use super::ReadFromJson;

        #[test]
        fn test_replica_map_file() {
            let p_map = PartitionMaps::read_from_json_file(
                "test-data/topics/replica_assignment.json",
                "dummytopic",
            )
            .expect("v1 not found");
            assert_eq!(p_map.maps().len(), 3);
            assert_eq!(p_map.maps()[0].id, 0);
            assert_eq!(p_map.maps()[0].replicas, vec![5001, 5002, 5003]);
        }

        #[test]
        fn test_mirror_map_file() {
            let m = MirrorConfig::read_from_json_file(
                "test-data/topics/mirror_assignment.json",
                "boats",
            )
            .expect("v1 not found");
            let core = match m {
                MirrorConfig::Home(m) => m,
                MirrorConfig::Remote(_) => panic!("not core"),
            };
            let partitions = core.partitions();
            assert_eq!(partitions.len(), 2);
            assert_eq!(
                partitions[0],
                HomePartitionConfig {
                    remote_cluster: "boat1".to_string(),
                    remote_replica: "boats-0".to_string(),
                    ..Default::default()
                }
            );
            assert_eq!(
                partitions[1],
                HomePartitionConfig {
                    remote_cluster: "boat2".to_string(),
                    remote_replica: "boats-0".to_string(),
                    ..Default::default()
                }
            );
        }
    }
}
