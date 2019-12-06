//!
//! # Kafka - Describe Topic Processing
//!
//! Communicates with Kafka Controller to retrieve desired Topic
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use serde::Serialize;
use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use crate::error::CliError;
use crate::common::{DescribeObjects, DescribeObjectHandler};
use crate::common::{EncoderOutputHandler, KeyValOutputHandler, TableOutputHandler};

use super::topic_metadata_kf::KfTopicMetadata;
use super::topic_metadata_kf::query_kf_topic_metadata;

use crate::topic::describe::DescribeTopicsConfig;

// -----------------------------------
// Data Structures (Serializable)
// -----------------------------------

type DescribeKfTopics = DescribeObjects<DescribeKfTopic>;

#[derive(Serialize, Debug)]
pub struct DescribeKfTopic {
    pub topic_metadata: KfTopicMetadata,
}

// -----------------------------------
// Process Request
// -----------------------------------

// Connect to Kafka Controller and query server for topic
pub fn process_kf_describe_topics(
    server_addr: SocketAddr,
    describe_topic_cfg: &DescribeTopicsConfig,
) -> Result<(), CliError> {
    // query none for empty topic_names array
    let topic_names = if describe_topic_cfg.topic_names.len() > 0 {
        Some(describe_topic_cfg.topic_names.clone())
    } else {
        None
    };

    let topics_metadata = query_kf_topic_metadata(server_addr, topic_names)?;
    let describe_topics = DescribeKfTopics::new(topics_metadata);

    // print table
    let output_type = &describe_topic_cfg.output;
    if output_type.is_table() {
        describe_topics.print_table()
    } else {
        describe_topics.display_encoding(output_type)
    }
}

// -----------------------------------
// Implement - DescribeKfTopics
// -----------------------------------

impl DescribeKfTopics {
    /// Convert vector of KfTopicMetadata to vector of DescribeTopic{KfTopicMetadata}
    fn new(mut topics_metadata: Vec<KfTopicMetadata>) -> Self {
        let mut describe_topics = vec![];

        while topics_metadata.len() > 0 {
            describe_topics.push(DescribeKfTopic {
                topic_metadata: topics_metadata.remove(0),
            });
        }

        DescribeKfTopics {
            label: "topic",
            label_plural: "topics",
            describe_objects: describe_topics,
        }
    }
}

// -----------------------------------
// Implement - EncoderOutputHandler
// -----------------------------------

impl EncoderOutputHandler for DescribeKfTopics {
    /// serializable data type
    type DataType = Vec<DescribeKfTopic>;

    /// serializable data to be encoded
    fn data(&self) -> &Vec<DescribeKfTopic> {
        &self.describe_objects
    }
}

// -----------------------------------
// Implement - DescribeObjectHandler
// -----------------------------------

impl DescribeObjectHandler for DescribeKfTopic {
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

impl TableOutputHandler for DescribeKfTopic {
    /// table header implementation
    fn header(&self) -> Row {
        row!["ID", "STATUS", "LEADER", "REPLICAS", "ISR"]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        vec![]
    }
    /// table content implementation
    fn content(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = vec![];
        if let Some(topic) = &self.topic_metadata.topic {
            for partition in &topic.partition_map {
                rows.push(row![
                    r -> partition.id,
                    c -> partition.status,
                    c -> partition.leader,
                    l -> format!("{:?}", partition.replicas),
                    l -> format!("{:?}", partition.isr),
                ]);
            }
        }

        rows
    }
}

// -----------------------------------
// Implement - KeyValOutputHandler
// -----------------------------------

impl KeyValOutputHandler for DescribeKfTopic {
    /// key value hash map implementation
    fn key_vals(&self) -> Vec<(String, Option<String>)> {
        let mut key_vals = Vec::new();
        if let Some(topic) = &self.topic_metadata.topic {
            key_vals.push(("Name".to_owned(), Some(self.topic_metadata.name.clone())));
            key_vals.push(("Internal".to_owned(), Some(topic.is_internal.to_string())));

            key_vals.push((
                "Partition Count".to_owned(),
                Some(topic.partitions.to_string()),
            ));
            key_vals.push((
                "Replication Factor".to_owned(),
                Some(topic.replication_factor.to_string()),
            ));
            key_vals.push(("Partition Replicas".to_owned(), None));
            key_vals.push(("-----------------".to_owned(), None));
        }
        key_vals
    }
}
