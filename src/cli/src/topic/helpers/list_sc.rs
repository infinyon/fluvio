//!
//! # Fluvio SC - List Topic Processing
//!
//! Retrieve all Topics and print to screen
//!

use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use log::debug;

use fluvio_client::ScClient;
use fluvio_client::SpuController;

use crate::error::CliError;
use crate::OutputType;
use crate::TableOutputHandler;
use crate::Terminal;
use crate::t_println;

use super::topic_metadata_sc::ScTopicMetadata;


type ListTopics = Vec<ScTopicMetadata>;

// -----------------------------------
// Process Request
// -----------------------------------

// Retrieve and print topics in desired format
pub async fn list_sc_topics<O>(
    out: std::sync::Arc<O>,
    mut client: ScClient<String>,
    output_type: OutputType,
) -> Result<(), CliError> 
    where O: Terminal
{

    let topics = client.topic_metadata(None).await?;
    let list_topics: Vec<ScTopicMetadata> = topics.into_iter().map(|t| ScTopicMetadata::new(t)).collect();
    debug!("topics retrieved: {:#?}",list_topics);
    format_response_output(out,list_topics, output_type)
}

/// Process server based on output type
fn format_response_output<O>(
    out: std::sync::Arc<O>,
    list_topics: ListTopics,
    output_type: OutputType,
) -> Result<(), CliError>
    where O: Terminal
 {
    if list_topics.len() > 0 {
        out.render_list(&list_topics,output_type)
    } else {
        t_println!(out,"No topics found");
        Ok(())
    }
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
        for topic_metadata in self.iter() {
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
        for topic_metadata in self.iter() {
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
