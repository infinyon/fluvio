use std::sync::Arc;
use structopt::StructOpt;

mod create;
mod delete;
mod describe;
mod list;

use create::CreateTopicOpt;
use delete::DeleteTopicOpt;
use describe::DescribeTopicsOpt;
use list::ListTopicsOpt;

use crate::{Result, Terminal};
use fluvio::Fluvio;

#[derive(Debug, StructOpt)]
#[structopt(name = "topic", about = "Topic operations")]
pub enum TopicCmd {
    /// Creates a Topic with the given name
    #[structopt(
        name = "create",
        template = crate::COMMAND_TEMPLATE,
    )]
    Create(CreateTopicOpt),

    /// Deletes a Topic with the given name
    #[structopt(
        name = "delete",
        template = crate::COMMAND_TEMPLATE,
    )]
    Delete(DeleteTopicOpt),

    /// Prints detailed information about a Topic
    #[structopt(
        name = "describe",
        template = crate::COMMAND_TEMPLATE,
    )]
    Describe(DescribeTopicsOpt),

    /// Lists all of the Topics in the cluster
    #[structopt(
        name = "list",
        template = crate::COMMAND_TEMPLATE,
    )]
    List(ListTopicsOpt),
}

impl TopicCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            TopicCmd::Create(create) => {
                create.process(fluvio).await?;
            }
            TopicCmd::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            TopicCmd::Describe(describe) => {
                describe.process(out, fluvio).await?;
            }
            TopicCmd::List(list) => {
                list.process(out, fluvio).await?;
            }
        }

        Ok(())
    }
}
