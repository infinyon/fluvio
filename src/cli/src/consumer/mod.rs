use std::sync::Arc;
use structopt::StructOpt;

pub mod error;
mod topic;
mod consume;
mod produce;
mod partition;

pub use topic::TopicCmd;
pub use consume::ConsumeOpt;
pub use produce::ProduceOpt;
pub use partition::PartitionCmd;

use crate::Result;
use fluvio::Fluvio;
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;

// For some reason this doc string is the one that gets used for the top-level help menu.
// Please don't change it unless you want to update the top-level help menu "about".
/// Fluvio command-line interface
#[derive(StructOpt, Debug)]
pub enum FluvioCmd {
    /// Read messages from a topic/partition
    #[structopt(name = "consume")]
    Consume(ConsumeOpt),

    /// Write messages to a topic/partition
    #[structopt(name = "produce")]
    Produce(ProduceOpt),

    /// Manage and view Topics
    ///
    /// A Topic is essentially the name of a stream which carries messages that
    /// are related to each other. Similar to the role of tables in a relational
    /// database, the names and contents of Topics will typically reflect the
    /// structure of the application domain they are used for.
    #[structopt(name = "topic")]
    Topic(TopicCmd),

    /// Manage and view Partitions
    ///
    /// Partitions are a way to divide the total traffic of a single Topic into
    /// separate streams which may be processed independently. Data sent to different
    /// partitions may be processed by separate SPUs on different computers. By
    /// dividing the load of a Topic evenly among partitions, you can increase the
    /// total throughput of the Topic.
    #[structopt(name = "partition")]
    Partition(PartitionCmd),
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
        }

        Ok(())
    }
}
