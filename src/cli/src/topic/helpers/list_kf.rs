//!
//! # Kafka - List Topic Processing
//!
//! Communicates with Kafka Controller to retrieve all Topics
//!

use serde::Serialize;
use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use flv_client::client::*;

use crate::error::CliError;
use crate::OutputType;
use crate::TableOutputHandler;
use crate::Terminal;

use super::topic_metadata_kf::KfTopicMetadata;

#[derive(Serialize, Debug)]
struct ListTopics {
    topics: Vec<KfTopicMetadata>,
}

// -----------------------------------
// Process Request
// -----------------------------------

// Retrieve and print topics in desired format
pub async fn list_kf_topics<O>(
    out: std::sync::Arc<O>,
    mut client: KfClient,
    output_type: OutputType,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let topics = client.topic_metadata(None).await?;
    let wrapper_topics: Vec<KfTopicMetadata> = topics
        .into_iter()
        .map(|t| KfTopicMetadata::new(t))
        .collect();

    out.describe_objects(&wrapper_topics, output_type)
}

// -----------------------------------
// Output Handlers
// -----------------------------------
impl TableOutputHandler for ListTopics {
    /// table header implementation
    fn header(&self) -> Row {
        row!["NAME", "INTERNAL", "PARTITIONS", "REPLICAS",]
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
                    c -> topic.is_internal.to_string(),
                    c -> topic.partitions.to_string(),
                    c -> topic.replication_factor.to_string(),
                ]);
            }
        }
        rows
    }
}
