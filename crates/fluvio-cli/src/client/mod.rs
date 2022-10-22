mod topic;
mod consume;
mod hub;
mod produce;
mod partition;
mod tableformat;
mod smartmodule;
mod derivedstream;

pub use metadata::client_metadata;
pub use cmd::FluvioCmd;
pub use tableformat::TableFormatConfig;

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
    use std::fmt::Debug;

    use clap::{Parser};
    use async_trait::async_trait;

    use fluvio::Fluvio;
    use fluvio::metrics::ClientMetrics;

    use crate::common::target::ClusterTarget;
    use crate::common::Terminal;
    use crate::Result;

    use super::derivedstream::DerivedStreamCmd;
    use super::smartmodule::SmartModuleCmd;
    use super::consume::ConsumeOpt;
    use super::produce::ProduceOpt;
    use super::topic::TopicCmd;
    use super::partition::PartitionCmd;
    use super::tableformat::TableFormatCmd;
    use super::hub::HubCmd;

    #[async_trait]
    pub trait AdminClient: Sized {
        /// handle the command based on target
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            target: ClusterTarget,
        ) -> Result<()> {
            let fluvio = target.connect().await?;
            self.process_client(out, &fluvio).await?;
            Ok(())
        }

        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()>;
    }

    #[async_trait]
    pub trait ConsumerClient: Sized {
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            target: ClusterTarget,
            metrics: Arc<ClientMetrics>,
        ) -> Result<()> {
            let fluvio = target.connect().await?;
            self.process_client(out, &fluvio, metrics).await?;
            Ok(())
        }

        /// process client
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
            metrics: Arc<ClientMetrics>,
        ) -> Result<()>;
    }

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
        #[clap(
            subcommand,
            name = "smartmodule",
            visible_alias = "sm",
            // FIXME: We should remove this alias when we bump the platform version to 10.x
            alias = "smart-module"
        )]
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

        /// Work with the SmartModule Hub
        #[clap(subcommand, name = "hub")]
        Hub(HubCmd),
    }

    impl FluvioCmd {
        /// Connect to Fluvio and pass the Fluvio client to the subcommand handlers.
        pub async fn process<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            target: ClusterTarget,
        ) -> Result<()> {
            let metrics = Arc::new(ClientMetrics::new());

            match self {
                Self::Consume(consume) => {
                    consume.process(out, target, metrics.clone()).await?;
                }
                Self::Produce(produce) => {
                    produce.process(out, target, metrics.clone()).await?;
                }
                Self::Topic(topic) => {
                    topic.process(out, target).await?;
                }
                Self::Partition(partition) => {
                    partition.process(out, target).await?;
                }
                Self::SmartModule(smartmodule) => {
                    smartmodule.process(out, target).await?;
                }
                Self::TableFormat(tableformat) => {
                    tableformat.process(out, target).await?;
                }
                Self::DerivedStream(derivedstream) => {
                    derivedstream.process(out, target).await?;
                }
                Self::Hub(hub) => {
                    hub.process(out, target).await?;
                }
            }

            Ok(())
        }
    }
}
