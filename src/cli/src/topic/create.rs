//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use log::debug;
use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_client::metadata::topic::TopicSpec;

use crate::error::CliError;
use crate::target::ClusterTarget;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateTopicOpt {
    /// Topic name
    #[structopt(value_name = "topic name")]
    topic: String,

    /// Number of partitions
    #[structopt(
        short = "p",
        long = "partitions",
        value_name = "partitions",
        default_value = "1",
        required_unless = "replica_assignment"
    )]
    partitions: i32,

    /// Replication factor per partition
    #[structopt(
        short = "r",
        long = "replication",
        value_name = "integer",
        default_value = "1",
        required_unless = "replica_assignment"
    )]
    replication: i16,

    /// Ignore racks while computing replica assignment
    #[structopt(
        short = "i",
        long = "ignore-rack-assignment",
        conflicts_with = "replica_assignment"
    )]
    ignore_rack_assigment: bool,

    /// Replica assignment file
    #[structopt(
        short = "f",
        long = "replica-assignment",
        value_name = "file.json",
        parse(from_os_str),
        conflicts_with = "partitions",
        conflicts_with = "replication"
    )]
    replica_assignment: Option<PathBuf>,

    /// Validates configuration, does not provision
    #[structopt(short = "d", long)]
    dry_run: bool,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl CreateTopicOpt {
    /// Validate cli options. Generate target-server and create-topic configuration.
    fn validate(self) -> Result<(ClusterConfig, (String, TopicSpec)), CliError> {
        use flv_client::metadata::topic::PartitionMaps;
        use flv_client::metadata::topic::TopicReplicaParam;
        use load::PartitionLoad;

        let target_server = self.target.load()?;

        let topic = if let Some(replica_assign_file) = &self.replica_assignment {
            TopicSpec::Assigned(
                PartitionMaps::file_decode(replica_assign_file).map_err(|err| {
                    IoError::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "cannot parse replica assignment file {:?}: {}",
                            replica_assign_file, err
                        ),
                    )
                })?,
            )
        } else {
            TopicSpec::Computed(TopicReplicaParam {
                partitions: self.partitions,
                replication_factor: self.replication as i32,
                ignore_rack_assignment: self.ignore_rack_assigment,
            })
        };

        // return server separately from config
        Ok((target_server, (self.topic, topic)))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process create topic cli request
pub async fn process_create_topic(opt: CreateTopicOpt) -> Result<String, CliError> {
    let dry_run = opt.dry_run;

    let (target_server, (name, topic_spec)) = opt.validate()?;

    debug!("creating topic: {} spec: {:#?}", name, topic_spec);

    let mut target = target_server.connect().await?;
    let mut admin = target.admin().await;

    admin.create(name.clone(), dry_run, topic_spec).await?;

    Ok(format!("topic \"{}\" created", name))
}

/// module to load partitions maps from file
mod load {

    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::fs::read_to_string;
    use std::path::Path;

    use flv_client::metadata::topic::PartitionMaps;

    pub trait PartitionLoad: Sized {
        fn file_decode<T: AsRef<Path>>(path: T) -> Result<Self, IoError>;
    }

    impl PartitionLoad for PartitionMaps {
        /// Read and decode the json file into Replica Assignment map
        fn file_decode<T: AsRef<Path>>(path: T) -> Result<Self, IoError> {
            let file_str: String = read_to_string(path)?;
            serde_json::from_str(&file_str)
                .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
        }
    }
}
