use std::sync::Arc;
use structopt::StructOpt;
use fluvio::Fluvio;

use crate::Result;
use crate::Terminal;
use crate::partition::list::ListPartitionOpt;

mod list;

#[derive(Debug, StructOpt)]
#[structopt(name = "partition", about = "Partition operations")]
pub enum PartitionCmd {
    /// List all of the Partitions in this cluster
    #[structopt(
        name = "list",
        template = crate::COMMAND_TEMPLATE,
    )]
    List(ListPartitionOpt),
}

impl PartitionCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }

        Ok(())
    }
}
