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
        display::format_response_output(out, topics, output_type)?;
        Ok(())
    }
}

mod display {

    use std::time::Duration;

    use humantime::{format_duration};
    use comfy_table::{Row, Cell, CellAlignment};
    use serde::Serialize;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::topic::TopicSpec;

    use crate::common::output::{OutputType, TableOutputHandler, Terminal, OutputError};
    use crate::common::t_println;

    #[derive(Serialize)]
    struct ListTopics(Vec<Metadata<TopicSpec>>);

    /// Process server based on output type
    pub fn format_response_output<O>(
        out: std::sync::Arc<O>,
        list_topics: Vec<Metadata<TopicSpec>>,
        output_type: OutputType,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        if !list_topics.is_empty() {
            let table_list = ListTopics(list_topics);
            out.render_list(&table_list, output_type)
        } else {
            t_println!(out, "No topics found");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListTopics {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from([
                "NAME",
                "TYPE",
                "PARTITIONS",
                "REPLICAS",
                "RETENTION TIME",
                "COMPRESSION",
                "STATUS",
                "REASON",
            ])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|metadata| -> Row {
                    let topic = &metadata.spec;

                    Row::from([
                        Cell::new(metadata.name.to_string()),
                        Cell::new(topic.type_label()),
                        Cell::new(topic.partitions_display()).set_alignment(CellAlignment::Left),
                        Cell::new(topic.replication_factor_display()),
                        Cell::new(format_duration(Duration::from_secs(
                            topic.retention_secs() as u64
                        ))),
                        Cell::new(topic.get_compression_type()),
                        Cell::new(metadata.status.resolution.to_string()),
                        Cell::new(metadata.status.reason.to_string()),
                    ])
                })
                .collect()
        }
    }
}
