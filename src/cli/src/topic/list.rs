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

use crate::Terminal;
use crate::Result;
use crate::common::OutputFormat;

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
        let mut admin = fluvio.admin().await;

        let topics = admin.list::<TopicSpec, _>(vec![]).await?;
        display::format_response_output(out, topics, output_type)?;
        Ok(())
    }
}

mod display {

    use prettytable::*;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::topic::TopicSpec;

    use crate::error::CliError;
    use crate::OutputType;
    use crate::TableOutputHandler;
    use crate::Terminal;
    use crate::t_println;

    type ListTopics = Vec<Metadata<TopicSpec>>;

    /// Process server based on output type
    pub fn format_response_output<O>(
        out: std::sync::Arc<O>,
        list_topics: ListTopics,
        output_type: OutputType,
    ) -> Result<(), CliError>
    where
        O: Terminal,
    {
        if !list_topics.is_empty() {
            out.render_list(&list_topics, output_type)
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
                "IGNORE-RACK",
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
            self.iter()
                .map(|metadata| {
                    let topic = &metadata.spec;
                    row![
                        l -> metadata.name,
                        c -> topic.type_label(),
                        c -> topic.partitions_display(),
                        c -> topic.replication_factor_display(),
                        c -> topic.ignore_rack_assign_display(),
                        c -> metadata.status.resolution.to_string(),
                        l -> metadata.status.reason
                    ]
                })
                .collect()
        }
    }
}
