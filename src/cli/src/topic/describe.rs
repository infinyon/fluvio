//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use std::sync::Arc;
use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::Result;
use crate::Terminal;
use crate::common::OutputFormat;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DescribeTopicsOpt {
    /// The name of the Topic to describe
    #[structopt(value_name = "name")]
    topic: String,

    #[structopt(flatten)]
    output: OutputFormat,
}

impl DescribeTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let topic = self.topic;
        let output_type = self.output.format;
        debug!("describe topic: {}, {}", topic, output_type);

        let mut admin = fluvio.admin().await;
        let topics = admin.list::<TopicSpec, _>(vec![topic]).await?;

        display::describe_topics(topics, output_type, out).await?;
        Ok(())
    }
}

mod display {

    use prettytable::Row;
    use prettytable::row;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::topic::TopicSpec;

    use crate::OutputType;
    use crate::Result;
    use crate::DescribeObjectHandler;
    use crate::{KeyValOutputHandler, TableOutputHandler};
    use crate::Terminal;

    type ListTopics = Vec<Metadata<TopicSpec>>;

    // Connect to Kafka Controller and query server for topic
    pub async fn describe_topics<O>(
        topics: ListTopics,
        output_type: OutputType,
        out: std::sync::Arc<O>,
    ) -> Result<()>
    where
        O: Terminal,
    {
        out.describe_objects(&topics, output_type)
    }

    impl DescribeObjectHandler for Metadata<TopicSpec> {
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

        fn validate(&self) -> Result<()> {
            Ok(())
        }
    }

    impl TableOutputHandler for Metadata<TopicSpec> {
        fn header(&self) -> Row {
            row![]
        }

        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            vec![]
        }
    }

    impl KeyValOutputHandler for Metadata<TopicSpec> {
        /// key value hash map implementation
        fn key_values(&self) -> Vec<(String, Option<String>)> {
            let mut key_values = Vec::new();
            let spec = &self.spec;
            let status = &self.status;

            key_values.push(("Name".to_owned(), Some(self.name.clone())));
            key_values.push(("Type".to_owned(), Some(spec.type_label().to_string())));
            match spec {
                TopicSpec::Computed(param) => {
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
                TopicSpec::Assigned(_partitions) => {
                    /*
                    key_values.push((
                        "Assigned Partitions".to_owned(),
                        Some(partitions.maps.clone()),
                    ));
                    */
                }
            }

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
