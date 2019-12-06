//!
//! # Fluvio SC - Describe Topic Processing
//!
//! Communicates with Fluvio Streaming Controller to retrieve desired Topic
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use serde::Serialize;
use prettytable::Row;
use prettytable::cell;
use prettytable::row;

use crate::error::CliError;
use crate::common::{DescribeObjects, DescribeObjectHandler};
use crate::common::{KeyValOutputHandler, TableOutputHandler, EncoderOutputHandler};

use super::topic_metadata_sc::ScTopicMetadata;
use super::topic_metadata_sc::query_sc_topic_metadata;

use crate::topic::describe::DescribeTopicsConfig;

// -----------------------------------
// Data Structures
// -----------------------------------

type DescribeScTopics = DescribeObjects<DescribeScTopic>;

#[derive(Serialize, Debug)]
pub struct DescribeScTopic {
    pub topic_metadata: ScTopicMetadata,
}

// -----------------------------------
// Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and query server for topics
pub fn process_sc_describe_topics(
    server_addr: SocketAddr,
    describe_topic_cfg: &DescribeTopicsConfig,
) -> Result<(), CliError> {
    // query none for empty topic_names array
    let topic_names = if describe_topic_cfg.topic_names.len() > 0 {
        Some(describe_topic_cfg.topic_names.clone())
    } else {
        None
    };

    let topics_metadata = query_sc_topic_metadata(server_addr, topic_names)?;
    let describe_topics = DescribeScTopics::new(topics_metadata);

    // print table
    let output_type = &describe_topic_cfg.output;
    if output_type.is_table() {
        describe_topics.print_table()
    } else {
        describe_topics.display_encoding(output_type)
    }
}

// -----------------------------------
// Implement - DescribeScTopics
// -----------------------------------

impl DescribeScTopics {
    /// Convert vector of ScTopicMetadata to vector of DescribeTopic{ScTopicMetadata}
    fn new(mut topics_metadata: Vec<ScTopicMetadata>) -> Self {
        let mut describe_topics = vec![];

        while topics_metadata.len() > 0 {
            describe_topics.push(DescribeScTopic {
                topic_metadata: topics_metadata.remove(0),
            });
        }

        DescribeScTopics {
            label: "topic",
            label_plural: "topics",
            describe_objects: describe_topics,
        }
    }
}

// -----------------------------------
// Implement - EncoderOutputHandler
// -----------------------------------

impl EncoderOutputHandler for DescribeScTopics {
    /// serializable data type
    type DataType = Vec<DescribeScTopic>;

    /// serializable data to be encoded
    fn data(&self) -> &Vec<DescribeScTopic> {
        &self.describe_objects
    }
}

// -----------------------------------
// Implement - DescribeObjectHandler
// -----------------------------------

impl DescribeObjectHandler for DescribeScTopic {
    fn is_ok(&self) -> bool {
        self.topic_metadata.topic.is_some()
    }

    fn is_error(&self) -> bool {
        self.topic_metadata.error.is_some()
    }

    /// validate topic
    fn validate(&self) -> Result<(), CliError> {
        let name = &self.topic_metadata.name;
        if let Some(error) = self.topic_metadata.error {
            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("topic '{}' {}", name, error.to_sentence()),
            )))
        } else if self.topic_metadata.topic.is_none() {
            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("topic '{}', undefined error", name),
            )))
        } else {
            Ok(())
        }
    }
}

// -----------------------------------
// Implement - TableOutputHandler
// -----------------------------------

impl TableOutputHandler for DescribeScTopic {
    /// table header implementation
    fn header(&self) -> Row {
        row!["ID", "LEADER", "REPLICAS", "LIVE-REPLICAS",]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        vec![]
    }

    /// table content implementation
    fn content(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = vec![];
        if let Some(topic) = &self.topic_metadata.topic {
            if let Some(ref partitions) = topic.partition_map {
                for partition in partitions {
                    rows.push(row![
                        r -> partition.id,
                        c -> partition.leader,
                        l -> format!("{:?}", partition.replicas),
                        l -> format!("{:?}", partition.live_replicas),
                    ]);
                }
            }
        }

        rows
    }
}

// -----------------------------------
// Implement - KeyValOutputHandler
// -----------------------------------

impl KeyValOutputHandler for DescribeScTopic {
    /// key value hash map implementation
    fn key_vals(&self) -> Vec<(String, Option<String>)> {
        let mut key_vals = Vec::new();
        if let Some(topic) = &self.topic_metadata.topic {
            let reason = if topic.reason.len() > 0 {
                topic.reason.clone()
            } else {
                "-".to_owned()
            };
            key_vals.push(("Name".to_owned(), Some(self.topic_metadata.name.clone())));
            key_vals.push(("Type".to_owned(), Some(topic.type_label().to_string())));
            if topic.assigned_partitions.is_some() {
                key_vals.push((
                    "Assigned Partitions".to_owned(),
                    Some(topic.assigned_partitions.as_ref().unwrap().clone()),
                ));
            }
            key_vals.push(("Partition Count".to_owned(), Some(topic.partitions_str())));
            key_vals.push((
                "Replication Factor".to_owned(),
                Some(topic.replication_factor_str()),
            ));
            key_vals.push((
                "Ignore Rack Assignment".to_owned(),
                Some(topic.ignore_rack_assign_str().to_string()),
            ));
            key_vals.push(("Status".to_owned(), Some(topic.status_label().to_string())));
            key_vals.push(("Reason".to_owned(), Some(reason)));
            key_vals.push(("Partition Map".to_owned(), None));
            key_vals.push(("-----------------".to_owned(), None));
        }
        key_vals
    }
}
