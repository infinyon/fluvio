//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::Duration;

use fluvio_controlplane_metadata::topic::ColumnDef;
use tracing::debug;
use clap::Parser;
use humantime::parse_duration;
use anyhow::{anyhow, Result};

use fluvio_types::PartitionCount;
use fluvio_types::ReplicationFactor;
use fluvio::metadata::topic::CleanupPolicy;
use fluvio::metadata::topic::ReplicaSpec;
use fluvio::metadata::topic::SegmentBasedPolicy;
use fluvio::metadata::topic::TopicStorageConfig;
use fluvio::metadata::topic::CompressionAlgorithm;

use fluvio_sc_schema::shared::validate_resource_name;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;
use crate::{CliError};

#[derive(Debug, Parser)]
pub struct CreateTopicOpt {
    /// The name of the Topic to create
    #[clap(value_name = "name")]
    topic: String,

    /// The number of Partitions to give the Topic
    ///
    /// Partitions are a way to divide the total traffic of a single Topic into
    /// separate streams which may be processed independently. Data sent to different
    /// partitions may be processed by separate SPUs on different computers. By
    /// dividing the load of a Topic evenly among partitions, you can increase the
    /// total throughput of the Topic.
    #[clap(
        short = 'p',
        long = "partitions",
        value_name = "partitions",
        default_value = "1"
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
    #[clap(
        short = 'r',
        long = "replication",
        value_name = "integer",
        default_value = "1"
    )]
    replication: i16,

    /// Ignore racks while computing replica assignment
    #[clap(
        short = 'i',
        long = "ignore-rack-assignment",
        conflicts_with = "replica_assignment"
    )]
    ignore_rack_assignment: bool,

    /// Replica assignment file
    #[clap(
        short = 'f',
        long = "replica-assignment",
        value_name = "file.json",
        value_parser,
        conflicts_with = "partitions",
        conflicts_with = "replication"
    )]
    replica_assignment: Option<PathBuf>,

    /// Validates configuration, does not provision
    #[clap(short = 'd', long)]
    dry_run: bool,

    #[clap(flatten)]
    setting: TopicConfigOpt,

    /// column mapping, this will map to duckdb columns, if this not specific, then default column (key,timestamp, value)
    /// this assume values json format
    /// Eg. -c  ph=contact.ph -c addr=contact.addr -e
    #[clap(
        short = 'c',
        long,
        value_parser=parse_key_val,
    )]
    pub columns: Vec<(String, String)>,
}

impl CreateTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let dry_run = self.dry_run;
        let (name, topic_spec) = self.validate()?;

        debug!("creating topic: {} spec: {:#?}", name, topic_spec);
        let admin = fluvio.admin().await;
        admin.create(name.clone(), dry_run, topic_spec).await?;
        println!("topic \"{name}\" created");

        Ok(())
    }

    /// Validate cli options. Generate target-server and create-topic configuration.
    fn validate(self) -> Result<(String, TopicSpec)> {
        use fluvio::metadata::topic::PartitionMaps;
        use fluvio::metadata::topic::TopicReplicaParam;
        use load::PartitionLoad;

        let replica_spec = if let Some(replica_assign_file) = &self.replica_assignment {
            ReplicaSpec::Assigned(PartitionMaps::file_decode(replica_assign_file).map_err(
                |err| {
                    IoError::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "cannot parse replica assignment file {replica_assign_file:?}: {err}"
                        ),
                    )
                },
            )?)
        } else {
            ReplicaSpec::Computed(TopicReplicaParam {
                partitions: self.partitions,
                replication_factor: self.replication as ReplicationFactor,
                ignore_rack_assignment: self.ignore_rack_assignment,
            })
        };

        if let Err(err) = validate_resource_name(&self.topic) {
            return Err(
                CliError::InvalidArg(format!("Invalid Topic name {}. {err}", self.topic,)).into(),
            );
        }

        let mut topic_spec: TopicSpec = replica_spec.into();

        topic_spec.set_columns(self.columns_mappings()?);

        if let Some(retention) = self.setting.retention_time {
            topic_spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: retention.as_secs() as u32,
            }));
        }

        if let Some(compression_type) = self.setting.compression_type {
            topic_spec.set_compression_type(compression_type);
        }

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

        // return server separately from config
        Ok((self.topic, topic_spec))
    }

    pub(crate) fn columns_mappings(&self) -> Result<Vec<ColumnDef>> {
        let mut columns = vec![];
        for (name, _) in &self.columns {
            columns.push(ColumnDef::from_dsl(&name)?);
        }
        Ok(columns)
    }
}

#[derive(Debug, Parser)]
pub struct TopicConfigOpt {
    /// Retention time (round to seconds)
    /// Ex: '1h', '2d 10s', '7 days' (default)
    #[clap(long, value_name = "time",value_parser=parse_duration)]
    retention_time: Option<Duration>,

    /// Segment size (by default measured in bytes)
    /// Ex: `2048`, '2 Ki', '10 MiB', `1 GB`
    #[clap(long, value_name = "bytes")]
    segment_size: Option<bytesize::ByteSize>,

    /// Compression configuration for topic
    #[clap(long, value_name = "compression")]
    compression_type: Option<CompressionAlgorithm>,

    /// Max partition size (by default measured in bytes)
    /// Ex: `2048`, '2 Ki', '10 MiB', `1 GB`
    #[clap(long, value_name = "bytes")]
    max_partition_size: Option<bytesize::ByteSize>,
}

/// module to load partitions maps from file
mod load {

    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::fs::read_to_string;
    use std::path::Path;

    use fluvio::metadata::topic::PartitionMaps;

    pub trait PartitionLoad: Sized {
        fn file_decode<T: AsRef<Path>>(path: T) -> Result<Self, IoError>;
    }

    impl PartitionLoad for PartitionMaps {
        /// Read and decode the json file into Replica Assignment map
        fn file_decode<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
            let file_str: String = read_to_string(path)?;
            serde_json::from_str(&file_str)
                .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{err}")))
        }
    }
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow!(format!("invalid KEY=value: no `=` found in `{s}`")))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
