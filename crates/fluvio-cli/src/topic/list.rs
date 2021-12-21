//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!

use std::sync::Arc;

use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::common::output::Terminal;
use crate::common::OutputFormat;
use crate::Result;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListTopicsOpt {
    /// Output
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let output_type = self.output.format;
        debug!("list topics {:#?} ", output_type);
        let admin = fluvio.admin().await;

        let topics = admin.list::<TopicSpec, _>(vec![]).await?;
        display::format_response_output(out, topics, output_type)?;
        Ok(())
    }
}

mod display {

    use std::time::Duration;

    use humantime::{format_duration};
    use prettytable::*;
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
            row![
                "NAME",
                "TYPE",
                "PARTITIONS",
                "REPLICAS",
                "RETENTION TIME",
                "STATUS",
                "REASON"
            ]
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
                    row![
                        l -> metadata.name,
                        c -> topic.type_label(),
                        c -> topic.partitions_display(),
                        c -> topic.replication_factor_display(),
                        c -> format_duration(Duration::from_secs(topic.retention_secs() as u64)),
                        c -> metadata.status.resolution.to_string(),
                        l -> metadata.status.reason
                    ]
                })
                .collect()
        }
    }
}
