//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!

use structopt::StructOpt;

use log::debug;

use flv_client::ClusterConfig;
use flv_client::metadata::topic::TopicSpec;

use crate::Terminal;
use crate::error::CliError;
use crate::OutputType;
use crate::target::ClusterTarget;

#[derive(Debug)]
pub struct ListTopicsConfig {
    pub output: OutputType,
}

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListTopicsOpt {
    /// Output
    #[structopt(
        short = "o",
        long = "output",
        value_name = "type",
        possible_values = &OutputType::variants(),
        case_insensitive = true,
    )]
    output: Option<OutputType>,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl ListTopicsOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ClusterConfig, OutputType), CliError> {
        let target_server = self.target.load()?;

        Ok((target_server, self.output.unwrap_or_default()))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list topics cli request
pub async fn process_list_topics<O>(
    out: std::sync::Arc<O>,
    opt: ListTopicsOpt,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let (target_server, output_type) = opt.validate()?;

    debug!("list topics {:#?} ", output_type);

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    let topics = admin.list::<TopicSpec,_>(vec![]).await?;
    display::format_response_output(out, topics, output_type)?;
    Ok("".to_owned())
}

mod display {

    use prettytable::*;

    use flv_client::metadata::objects::Metadata;
    use flv_client::metadata::topic::TopicSpec;

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
        if list_topics.len() > 0 {
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
