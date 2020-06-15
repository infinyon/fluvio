//!
//! # Fluvio SC - Describe Topic Processing
//!
//! Communicates with Fluvio Streaming Controller to retrieve desired Topic
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use prettytable::Row;
use prettytable::cell;
use prettytable::row;

use flv_client::client::*;
use flv_client::metadata::topic::TopicMetadata;

use crate::OutputType;
use crate::error::CliError;
use crate::DescribeObjectHandler;
use crate::{KeyValOutputHandler, TableOutputHandler};
use crate::Terminal;

// Connect to Kafka Controller and query server for topic
pub async fn describe_sc_topics<O>(
    mut client: ScClient,
    topics: Vec<String>,
    output_type: OutputType,
    out: std::sync::Arc<O>,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let topic_args = if topics.len() > 0 { Some(topics) } else { None };
    // query none for empty topic_names array
    let topics = client.topic_metadata(topic_args).await?;

    out.describe_objects(&topics, output_type)
}

impl DescribeObjectHandler for TopicMetadata {
    fn label() -> &'static str {
        "topic"
    }

    fn label_plural() -> &'static str {
        "topics"
    }

    fn is_ok(&self) -> bool {
        self.topic.is_some()
    }

    fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// validate topic
    fn validate(&self) -> Result<(), CliError> {
        let name = &self.name;
        if let Some(error) = self.error {
            Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("topic '{}' {}", name, error.to_sentence()),
            )))
        } else if self.topic.is_none() {
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

impl TableOutputHandler for TopicMetadata {
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
        if let Some(topic) = &self.topic {
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

impl KeyValOutputHandler for TopicMetadata {
    /// key value hash map implementation
    fn key_values(&self) -> Vec<(String, Option<String>)> {
        let mut key_values = Vec::new();
        if let Some(topic) = &self.topic {
            let reason = if topic.reason.len() > 0 {
                topic.reason.clone()
            } else {
                "-".to_owned()
            };
            key_values.push(("Name".to_owned(), Some(self.name.clone())));
            key_values.push(("Type".to_owned(), Some(topic.type_label().to_string())));
            if topic.assigned_partitions.is_some() {
                key_values.push((
                    "Assigned Partitions".to_owned(),
                    Some(topic.assigned_partitions.as_ref().unwrap().clone()),
                ));
            }
            key_values.push(("Partition Count".to_owned(), Some(topic.partitions_str())));
            key_values.push((
                "Replication Factor".to_owned(),
                Some(topic.replication_factor_str()),
            ));
            key_values.push((
                "Ignore Rack Assignment".to_owned(),
                Some(topic.ignore_rack_assign_str().to_string()),
            ));
            key_values.push(("Status".to_owned(), Some(topic.status_label().to_string())));
            key_values.push(("Reason".to_owned(), Some(reason)));
            key_values.push(("Partition Map".to_owned(), None));
            key_values.push(("-----------------".to_owned(), None));
        }
        key_values
    }
}
