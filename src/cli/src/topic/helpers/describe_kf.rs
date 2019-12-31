//!
//! # Kafka - Describe Topic Processing
//!
//! Communicates with Kafka Controller to retrieve desired Topic
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use fluvio_client::SpuController;
use fluvio_client::KfClient;

use crate::OutputType;
use crate::error::CliError;
use crate::DescribeObjectHandler;
use crate::{KeyValOutputHandler, TableOutputHandler};
use crate::Terminal;


use super::KfTopicMetadata;



// Connect to Kafka Controller and query server for topic
pub async fn describe_kf_topics<O>(
    mut client: KfClient<String>,
    topics: Vec<String>,
    output_type: OutputType,
    out: std::sync::Arc<O>
) -> Result<(), CliError>
    where O: Terminal
{
    let topic_args = if topics.len() > 0 {
        Some(topics)
    } else {
        None
    };

    // query none for empty topic_names array
    let topics = client.topic_metadata(topic_args).await?;

    let wrapper_topics: Vec<KfTopicMetadata> = topics.into_iter().map(|t| KfTopicMetadata::new(t)).collect();

    out.describe_objects(&wrapper_topics,output_type)
}


impl DescribeObjectHandler for KfTopicMetadata {

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

impl TableOutputHandler for KfTopicMetadata {
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
        if let Some(topic) = &self.topic {
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


impl KeyValOutputHandler for KfTopicMetadata {
    /// key value hash map implementation
    fn key_values(&self) -> Vec<(String, Option<String>)> {
        let mut key_values = Vec::new();
        if let Some(topic) = &self.topic {
            key_values.push(("Name".to_owned(), Some(self.name.clone())));
            key_values.push(("Internal".to_owned(), Some(topic.is_internal.to_string())));

            key_values.push((
                "Partition Count".to_owned(),
                Some(topic.partitions.to_string()),
            ));
            key_values.push((
                "Replication Factor".to_owned(),
                Some(topic.replication_factor.to_string()),
            ));
            key_values.push(("Partition Replicas".to_owned(), None));
            key_values.push(("-----------------".to_owned(), None));
        }
        key_values
    }
}
