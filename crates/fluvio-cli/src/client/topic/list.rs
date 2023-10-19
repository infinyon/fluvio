//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!

use std::sync::Arc;

use clap::Parser;
use tracing::debug;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::common::output::Terminal;
use crate::common::OutputFormat;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct ListTopicsOpt {
    /// Output
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let output_type = self.output.format;
        debug!("list topics {:#?} ", output_type);
        let admin = fluvio.admin().await;
        let topics = admin.all::<TopicSpec>().await?;
        display::format_response_output(out, topics, output_type, fluvio)?;
        Ok(())
    }
}

mod display {
    use std::time::{Duration, UNIX_EPOCH};

    use comfy_table::{Row, Cell, CellAlignment};
    use humantime::{format_duration, format_rfc3339_millis};
    use serde::Serialize;

    use fluvio::{
        Fluvio,
        metadata::{objects::Metadata, topic::TopicSpec},
    };
    use fluvio_future::task::run_block_on;

    use crate::common::output::{OutputType, TableOutputHandler, Terminal, OutputError};
    use crate::common::t_println;
    use super::utils::latest_record_timestamp;

    #[derive(Serialize)]
    struct ListTopics<'a> {
        inner: Vec<Metadata<TopicSpec>>,

        #[serde(skip_serializing)]
        fluvio: &'a Fluvio,
    }

    /// Process server based on output type
    pub fn format_response_output<O>(
        out: std::sync::Arc<O>,
        list_topics: Vec<Metadata<TopicSpec>>,
        output_type: OutputType,
        fluvio: &Fluvio,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        if !list_topics.is_empty() {
            let table_list = ListTopics {
                inner: list_topics,
                fluvio,
            };
            out.render_list(&table_list, output_type)
        } else {
            t_println!(out, "No topics found");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListTopics<'_> {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from([
                "NAME",
                "TYPE",
                "PARTITIONS",
                "REPLICAS",
                "RETENTION TIME",
                "COMPRESSION",
                "DEDUPLICATION",
                "STATUS",
                "LAST MODIFIED",
                "REASON",
            ])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.inner
                .iter()
                .map(|metadata| -> Row {
                    let topic = &metadata.spec;

                    let topic_name = metadata.name.to_string();

                    let modified_at = run_block_on(async move {
                        latest_record_timestamp(topic_name, self.fluvio)
                            .await
                            .map(|ts| {
                                let ts_ms = UNIX_EPOCH
                                    + u64::try_from(ts)
                                        .map(Duration::from_millis)
                                        .unwrap_or_default();
                                format!("{}", format_rfc3339_millis(ts_ms))
                            })
                    });

                    Row::from([
                        Cell::new(metadata.name.to_string()),
                        Cell::new(topic.type_label()),
                        Cell::new(topic.partitions_display()).set_alignment(CellAlignment::Left),
                        Cell::new(topic.replication_factor_display()),
                        Cell::new(format_duration(Duration::from_secs(
                            topic.retention_secs() as u64
                        ))),
                        Cell::new(topic.get_compression_type()),
                        Cell::new(
                            topic
                                .get_deduplication()
                                .map(|d| d.filter.transform.uses.as_str())
                                .unwrap_or("none"),
                        ),
                        Cell::new(metadata.status.resolution.resolution_label()),
                        Cell::new(modified_at.unwrap_or_else(String::new)),
                        Cell::new(metadata.status.reason.to_string()),
                    ])
                })
                .collect()
        }
    }
}

mod utils {
    use fluvio::{
        consumer::{ConsumerConfigBuilder, PartitionSelectionStrategy},
        Fluvio, Offset,
    };
    use fluvio_types::Timestamp;
    use futures_util::stream::StreamExt;

    /// A utility function to get the timestamp of the `Offset::from_end(1)` record across all
    /// partitions for a given `topic_name`.
    pub async fn latest_record_timestamp(topic_name: String, fluvio: &Fluvio) -> Option<Timestamp> {
        let consumer = fluvio
            .consumer(PartitionSelectionStrategy::All(topic_name))
            .await
            .ok()?;

        let consumer_config = ConsumerConfigBuilder::default()
            .disable_continuous(true)
            .build()
            .ok()?;

        let stream_future = consumer.stream_with_config(Offset::from_end(1), consumer_config);

        let mut stream = stream_future.await.ok()?;

        if let Some(Ok(record)) = stream.next().await {
            Some(record.timestamp())
        } else {
            None
        }
    }
}
