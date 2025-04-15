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
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_sc_schema::objects::{ListFilters, ListRequest, Metadata};
use crate::common::output::Terminal;
use crate::common::OutputFormat;
use crate::client::partition::list::display as display_partition;

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

    #[arg(long, short, required = false)]
    system: bool,
}

impl DescribeTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let topic = self.topic;
        let output_type = self.output.format;
        debug!("describe topic: {}, {:?}", topic, output_type);

        let admin = fluvio.admin().await;

        let topics_with_system = admin
            .list_with_config::<TopicSpec, String>(
                ListRequest::new(ListFilters::from(topic.as_str()), true).system(self.system),
            )
            .await?;

        let partitions = admin
            .list_with_config::<PartitionSpec, String>(
                ListRequest::new(ListFilters::from(topic.as_str()), true).system(self.system),
            )
            .await?;

        let filtered_partitions = filter_partition_by_topic(topic, partitions);
        display::describe_topics(topics_with_system, output_type.clone(), out.clone()).await?;
        display_partition::format_partition_response_output(out, filtered_partitions, output_type)?;
        Ok(())
    }
}

fn filter_partition_by_topic(
    topic: String,
    partitions: Vec<Metadata<PartitionSpec>>,
) -> Vec<Metadata<PartitionSpec>> {
    partitions
        .clone()
        .into_iter()
        .filter(|partition| {
            if let Some(index) = partition.name.rfind('-') {
                let base_name = &partition.name[..index];
                base_name == topic
            } else {
                partition.name == topic
            }
        })
        .collect::<Vec<_>>()
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
                ReplicaSpec::Mirror(_config) => {}
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

#[cfg(test)]
mod tests {
    use fluvio_controlplane_metadata::partition::PartitionStatus;
    use super::*;
    use fluvio_sc_schema::partition::PartitionSpec;

    #[test]
    fn test_filter_partition_by_topic_1() {
        // partition 1
        let partition_with_topic_1: Vec<Metadata<PartitionSpec>> =
            vec![Metadata::<PartitionSpec> {
                name: "topic-1".to_string(),
                spec: PartitionSpec::new(0, vec![0, 1]),
                status: PartitionStatus::default(),
            }];

        let filtered_topic_1 =
            filter_partition_by_topic("topic".to_string(), partition_with_topic_1.clone());
        assert_eq!(filtered_topic_1.len(), 1);
        assert_eq!(filtered_topic_1[0].name, "topic-1");
        assert_eq!(filtered_topic_1[0].spec.leader, 0);
        assert_eq!(filtered_topic_1[0].status, PartitionStatus::default());
    }

    #[test]
    fn test_filter_partition_by_topic_2() {
        // partition 2
        let partition_with_topic_2: Vec<Metadata<PartitionSpec>> =
            vec![Metadata::<PartitionSpec> {
                name: "topic-2".to_string(),
                spec: PartitionSpec::new(0, vec![0, 1]),
                status: PartitionStatus::default(),
            }];

        let filtered_topic_2 =
            filter_partition_by_topic("topic".to_string(), partition_with_topic_2.clone());
        assert_eq!(filtered_topic_2.len(), 1);
        assert_eq!(filtered_topic_2[0].name, "topic-2");
        assert_eq!(filtered_topic_2[0].spec.leader, 0);
        assert_eq!(filtered_topic_2[0].status, PartitionStatus::default());
    }

    #[test]
    fn test_filter_partition_by_kv_topic() {
        let partition_with_kv_topic: Vec<Metadata<PartitionSpec>> =
            vec![Metadata::<PartitionSpec> {
                name: "kv-topic-2".to_string(),
                spec: PartitionSpec::new(0, vec![0, 1]),
                status: PartitionStatus::default(),
            }];

        // result should be empty
        let filtered_kv_topic =
            filter_partition_by_topic("topic".to_string(), partition_with_kv_topic.clone());
        assert_eq!(filtered_kv_topic.len(), 0);
    }
}
