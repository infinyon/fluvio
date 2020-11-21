//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;
use crate::Result;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateTopicOpt {
    /// The name of the Topic to create
    #[structopt(value_name = "name")]
    topic: String,

    /// The number of Partitions to give the Topic
    ///
    /// Partitions are a way to divide the total traffic of a single Topic into
    /// separate streams which may be processed independently. Data sent to different
    /// partitions may be processed by separate SPUs on different computers. By
    /// dividing the load of a Topic evenly among partitions, you can increase the
    /// total throughput of the Topic.
    #[structopt(
        short = "p",
        long = "partitions",
        value_name = "partitions",
        default_value = "1"
    )]
    partitions: i32,

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
    #[structopt(
        short = "r",
        long = "replication",
        value_name = "integer",
        default_value = "1"
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
}

impl CreateTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let dry_run = self.dry_run;
        let (name, topic_spec) = self.validate()?;

        debug!("creating topic: {} spec: {:#?}", name, topic_spec);
        let mut admin = fluvio.admin().await;
        admin.create(name.clone(), dry_run, topic_spec).await?;
        println!("topic \"{}\" created", name);

        Ok(())
    }

    /// Validate cli options. Generate target-server and create-topic configuration.
    fn validate(self) -> Result<(String, TopicSpec)> {
        use fluvio::metadata::topic::PartitionMaps;
        use fluvio::metadata::topic::TopicReplicaParam;
        use load::PartitionLoad;

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
        Ok((self.topic, topic))
    }
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
                .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))
        }
    }
}
