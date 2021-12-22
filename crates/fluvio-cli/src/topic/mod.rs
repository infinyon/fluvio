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

use fluvio::Fluvio;

use crate::Result;
use crate::common::COMMAND_TEMPLATE;
use crate::common::output::Terminal;
use crate::common::FluvioExtensionMetadata;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "topic", about = "Topic operations")]
pub enum TopicCmd {
    /// Create a Topic with the given name
    #[structopt(
        name = "create",
        template = COMMAND_TEMPLATE,
    )]
    Create(CreateTopicOpt),

    /// Delete a Topic with the given name
    #[structopt(
        name = "delete",
        template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteTopicOpt),

    /// Print detailed information about a Topic
    #[structopt(
        name = "describe",
        template = COMMAND_TEMPLATE,
    )]
    Describe(DescribeTopicsOpt),

    /// List all of the Topics in the cluster
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListTopicsOpt),
}

impl TopicCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::Create(create) => {
                create.process(fluvio).await?;
            }
            Self::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            Self::Describe(describe) => {
                describe.process(out, fluvio).await?;
            }
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }

        Ok(())
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            title: "topic".into(),
            package: Some("fluvio/fluvio".parse().unwrap()),
            description: "Topic Operations".into(),
            version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        }
    }
}
