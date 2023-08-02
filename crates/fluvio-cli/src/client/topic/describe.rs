//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use std::sync::Arc;

use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::common::output::Terminal;
use crate::common::OutputFormat;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DescribeTopicsOpt {
    /// The name of the Topic to describe
    #[arg(value_name = "name")]
    topic: String,

    #[clap(flatten)]
    output: OutputFormat,
}

impl DescribeTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let topic = self.topic;
        let output_type = self.output.format;
        debug!("describe topic: {}, {:?}", topic, output_type);

        let admin = fluvio.admin().await;
        let topics = admin.list::<TopicSpec, _>(vec![topic]).await?;

        display::describe_topics(topics, output_type, out).await?;
        Ok(())
    }
}

mod display {

    use fluvio::metadata::topic::ReplicaSpec;
    use comfy_table::Row;
    use humantime::format_duration;
    use serde::Serialize;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::topic::TopicSpec;

    use crate::common::output::{
        OutputType, OutputError, DescribeObjectHandler, KeyValOutputHandler, TableOutputHandler,
        Terminal,
    };

    #[allow(clippy::redundant_closure)]
    // Connect to Kafka Controller and query server for topic
    pub async fn describe_topics<O>(
        topics: Vec<Metadata<TopicSpec>>,
        output_type: OutputType,
        out: std::sync::Arc<O>,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        let topic_list: Vec<TopicMetadata> = topics.into_iter().map(|m| TopicMetadata(m)).collect();
        out.describe_objects(&topic_list, output_type)
    }

    #[derive(Serialize, Clone)]
    struct TopicMetadata(Metadata<TopicSpec>);

    impl DescribeObjectHandler for TopicMetadata {
        fn label() -> &'static str {
            "topic"
        }

        fn label_plural() -> &'static str {
            "topics"
        }

        fn is_ok(&self) -> bool {
            true
        }

        fn is_error(&self) -> bool {
            false
        }

        fn validate(&self) -> Result<(), OutputError> {
            Ok(())
        }
    }

    impl TableOutputHandler for TopicMetadata {
        fn header(&self) -> Row {
            Row::new()
        }

        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            vec![]
        }
    }

    impl KeyValOutputHandler for TopicMetadata {
        /// key value hash map implementation
        fn key_values(&self) -> Vec<(String, Option<String>)> {
            let mut key_values = Vec::new();
            let spec = &self.0.spec;
            let status = &self.0.status;

            key_values.push(("Name".to_owned(), Some(self.0.name.clone())));
            key_values.push(("Type".to_owned(), Some(spec.type_label().to_string())));
            match spec.replicas() {
                ReplicaSpec::Computed(param) => {
                    key_values.push((
                        "Partition Count".to_owned(),
                        Some(param.partitions.to_string()),
                    ));
                    key_values.push((
                        "Replication Factor".to_owned(),
                        Some(param.replication_factor.to_string()),
                    ));
                    key_values.push((
                        "Ignore Rack Assignment".to_owned(),
                        Some(param.ignore_rack_assignment.to_string()),
                    ));
                }
                ReplicaSpec::Assigned(_partitions) => {
                    /*
                    key_values.push((
                        "Assigned Partitions".to_owned(),
                        Some(partitions.maps.clone()),
                    ));
                    */
                }
                ReplicaSpec::Mirror(_partitions) => {}
            }

            if let Some(dedup) = spec.get_deduplication() {
                key_values.push((
                    "Deduplication Filter".to_owned(),
                    Some(dedup.filter.transform.uses.clone()),
                ));
                key_values.push((
                    "Deduplication Count Bound".to_owned(),
                    Some(dedup.bounds.count)
                        .filter(|c| *c != 0)
                        .as_ref()
                        .map(ToString::to_string),
                ));
                key_values.push((
                    "Deduplication Age Bound".to_owned(),
                    dedup.bounds.age.map(|a| format_duration(a).to_string()),
                ));
            };

            key_values.push((
                "Status".to_owned(),
                Some(status.resolution.resolution_label().to_string()),
            ));
            key_values.push(("Reason".to_owned(), Some(status.reason.clone())));

            key_values.push(("-----------------".to_owned(), None));

            key_values
        }
    }
}
