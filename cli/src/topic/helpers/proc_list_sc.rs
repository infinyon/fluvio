//!
//! # Fluvio SC - List Topic Processing
//!
//! Retrieve all Topics and print to screen
//!

use std::net::SocketAddr;

use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use crate::error::CliError;
use crate::common::OutputType;
use crate::common::{EncoderOutputHandler, TableOutputHandler};

use super::topic_metadata_sc::ScTopicMetadata;
use super::topic_metadata_sc::query_sc_topic_metadata;

use crate::topic::list::ListTopicsConfig;

// -----------------------------------
// ListTopics Data Structure
// -----------------------------------

#[derive(Debug)]
struct ListTopics {
    topics: Vec<ScTopicMetadata>,
}

// -----------------------------------
// Process Request
// -----------------------------------

// Retrieve and print topics in desired format
pub fn process_list_topics(
    server_addr: SocketAddr,
    list_topic_cfg: &ListTopicsConfig,
) -> Result<(), CliError> {
    let topics = query_sc_topic_metadata(server_addr, None)?;
    let list_topics = ListTopics { topics };

    format_response_output(&list_topics, &list_topic_cfg.output)
}

/// Process server based on output type
fn format_response_output(
    list_topics: &ListTopics,
    output_type: &OutputType,
) -> Result<(), CliError> {
    // expecting array with one or more elements
    if list_topics.topics.len() > 0 {
        if output_type.is_table() {
            list_topics.display_errors();
            list_topics.display_table(false);
        } else {
            list_topics.display_encoding(output_type)?;
        }
    } else {
        println!("No topics found");
    }
    Ok(())
}

// -----------------------------------
// Output Handlers
// -----------------------------------
impl TableOutputHandler for ListTopics {
    /// table header implementation
    fn header(&self) -> Row {
        row![
            "NAME",
            "TYPE",
            "PARTITIONS",
            "REPLICAS",
            "IGNORE-RACK",
            "STATUS",
            "REASON"
        ]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        let mut errors = vec![];
        for topic_metadata in &self.topics {
            if let Some(error) = &topic_metadata.error {
                errors.push(format!(
                    "Topic '{}': {}",
                    topic_metadata.name,
                    error.to_sentence()
                ));
            }
        }
        errors
    }

    /// table content implementation
    fn content(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = vec![];
        for topic_metadata in &self.topics {
            if let Some(topic) = &topic_metadata.topic {
                rows.push(row![
                    l -> topic_metadata.name,
                    c -> topic.type_label(),
                    c -> topic.partitions_str(),
                    c -> topic.replication_factor_str(),
                    c -> topic.ignore_rack_assign_str(),
                    c -> topic.status_label(),
                    l -> topic.reason,
                ]);
            }
        }
        rows
    }
}

impl EncoderOutputHandler for ListTopics {
    /// serializable data type
    type DataType = Vec<ScTopicMetadata>;

    /// serializable data to be encoded
    fn data(&self) -> &Vec<ScTopicMetadata> {
        &self.topics
    }
}
