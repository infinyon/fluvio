mod topic;
mod consume;
mod produce;
mod partition;
mod tableformat;
mod smartmodule;
mod derivedstream;

pub use metadata::client_metadata;
pub use cmd::FluvioCmd;

mod metadata {

    use fluvio_extension_common::FluvioExtensionMetadata;

    use super::topic::TopicCmd;
    use super::partition::PartitionCmd;
    use super::produce::ProduceOpt;
    use super::consume::ConsumeOpt;

    /// return list of metadata associated with client
    pub fn client_metadata() -> Vec<FluvioExtensionMetadata> {
        vec![
            TopicCmd::metadata(),
            PartitionCmd::metadata(),
            ProduceOpt::metadata(),
            ConsumeOpt::metadata(),
        ]
    }
}

mod cmd {

    use std::sync::Arc;


    use clap::{Parser};


    use fluvio::Fluvio;

    
    pub use fluvio_channel::{FLUVIO_RELEASE_CHANNEL, FLUVIO_EXTENSIONS_DIR, FLUVIO_IMAGE_TAG_STRATEGY};


    use crate::common::target::ClusterTarget;

    use crate::common::Terminal;

    use super::derivedstream::DerivedStreamCmd;
    use super::smartmodule::SmartModuleCmd;
    use super::consume::ConsumeOpt;
    use super::produce::ProduceOpt;
    use super::topic::TopicCmd;
    use super::partition::PartitionCmd;
    use super::tableformat::TableFormatCmd;

    // For some reason this doc string is the one that gets used for the top-level help menu.
    // Please don't change it unless you want to update the top-level help menu "about".
    /// Fluvio command-line interface
    #[derive(Parser, Debug)]
    pub enum FluvioCmd {
        /// Read messages from a topic/partition
        #[clap(name = "consume")]
        Consume(Box<ConsumeOpt>),

        /// Write messages to a topic/partition
        #[clap(name = "produce")]
        Produce(ProduceOpt),

        /// Manage and view Topics
        ///
        /// A Topic is essentially the name of a stream which carries messages that
        /// are related to each other. Similar to the role of tables in a relational
        /// database, the names and contents of Topics will typically reflect the
        /// structure of the application domain they are used for.
        #[clap(subcommand, name = "topic")]
        Topic(TopicCmd),

        /// Manage and view Partitions
        ///
        /// Partitions are a way to divide the total traffic of a single Topic into
        /// separate streams which may be processed independently. Data sent to different
        /// partitions may be processed by separate SPUs on different computers. By
        /// dividing the load of a Topic evenly among partitions, you can increase the
        /// total throughput of the Topic.
        #[clap(subcommand, name = "partition")]
        Partition(PartitionCmd),

        /// Create and manage SmartModules
        ///
        /// SmartModules are compiled WASM modules used to create SmartModules.
        #[clap(subcommand, name = "smart-module", visible_alias = "sm")]
        SmartModule(SmartModuleCmd),

        /// Create a TableFormat display specification
        ///
        /// Used with the consumer output type `full_table` to
        /// describe how to render JSON data in a tabular form
        #[clap(subcommand, name = "table-format", visible_alias = "tf")]
        TableFormat(TableFormatCmd),

        /// Create and manage DerivedStreams
        ///
        /// Use topics, SmartModules or other DerivedStreams
        /// to build a customized stream to consume
        #[clap(subcommand, name = "derived-stream", visible_alias = "ds")]
        DerivedStream(DerivedStreamCmd),
    }

    impl FluvioCmd {
        /// Connect to Fluvio and pass the Fluvio client to the subcommand handlers.
        pub async fn process<O: Terminal>(self, out: Arc<O>, target: ClusterTarget) -> Result<()> {
            let fluvio_config = target.load()?;
            let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;

            match self {
                Self::Consume(consume) => {
                    consume.process(&fluvio).await?;
                }
                Self::Produce(produce) => {
                    produce.process(&fluvio).await?;
                }
                Self::Topic(topic) => {
                    topic.process(out, &fluvio).await?;
                }
                Self::Partition(partition) => {
                    partition.process(out, &fluvio).await?;
                }
                Self::SmartModule(smartmodule) => {
                    smartmodule.process(out, &fluvio).await?;
                }
                Self::TableFormat(tableformat) => {
                    tableformat.process(out, &fluvio).await?;
                }
                Self::DerivedStream(derivedstream) => {
                    derivedstream.process(out, &fluvio).await?;
                }
            }

            Ok(())
        }
    }

}
