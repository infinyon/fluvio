//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use log::debug;
use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_client::metadata::topic::TopicSpec;

use crate::target::ClusterTarget;

use crate::Terminal;
use crate::error::CliError;
use crate::OutputType;
use crate::common::OutputFormat;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DescribeTopicsOpt {
    /// Topic filter
    #[structopt(value_name = "string")]
    topic: String,

    #[structopt(flatten)]
    output: OutputFormat,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl DescribeTopicsOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ClusterConfig, (String, OutputType)), CliError> {
        let target_server = self.target.load()?;

        // transfer config parameters
        let (topic, output) = (self.topic, self.output.as_output());

        // return server separately from topic result
        Ok((target_server, (topic, output)))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process describe topic cli request
pub async fn process_describe_topics<O>(
    out: std::sync::Arc<O>,
    opt: DescribeTopicsOpt,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let (target_server, (topic, output_type)) = opt.validate()?;

    debug!("describe topic: {}, {}", topic, output_type);

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    let topics = admin.list::<TopicSpec,_>(vec![topic]).await?;

    display::describe_topics(topics, output_type, out).await?;
    Ok("".to_owned())
}

mod display {

    use prettytable::Row;
    use prettytable::row;

    use flv_client::metadata::objects::Metadata;
    use flv_client::metadata::topic::TopicSpec;

    use crate::OutputType;
    use crate::error::CliError;
    use crate::DescribeObjectHandler;
    use crate::{KeyValOutputHandler, TableOutputHandler};
    use crate::Terminal;

    type ListTopics = Vec<Metadata<TopicSpec>>;

    // Connect to Kafka Controller and query server for topic
    pub async fn describe_topics<O>(
        topics: ListTopics,
        output_type: OutputType,
        out: std::sync::Arc<O>,
    ) -> Result<(), CliError>
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

        fn validate(&self) -> Result<(), CliError> {
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
