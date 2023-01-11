mod create;
mod delete;
mod describe;
mod list;

pub use cmd::TopicCmd;

mod cmd {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;
    use anyhow::Result;

    use fluvio::Fluvio;

    use crate::client::cmd::ClientCmd;
    use crate::common::COMMAND_TEMPLATE;
    use crate::common::output::Terminal;
    use crate::common::FluvioExtensionMetadata;

    use super::create::CreateTopicOpt;
    use super::delete::DeleteTopicOpt;
    use super::describe::DescribeTopicsOpt;
    use super::list::ListTopicsOpt;

    #[derive(Debug, Parser)]
    #[clap(name = "topic", about = "Topic operations")]
    pub enum TopicCmd {
        /// Create a Topic with the given name
        #[clap(
            name = "create",
            help_template = COMMAND_TEMPLATE,
        )]
        Create(CreateTopicOpt),

        /// Delete one or more Topics with the given name(s)
        #[clap(
            name = "delete",
            help_template = COMMAND_TEMPLATE,
        )]
        Delete(DeleteTopicOpt),

        /// Print detailed information about a Topic
        #[clap(
            name = "describe",
            help_template = COMMAND_TEMPLATE,
        )]
        Describe(DescribeTopicsOpt),

        /// List all of the Topics in the cluster
        #[clap(
            name = "list",
            help_template = COMMAND_TEMPLATE,
        )]
        List(ListTopicsOpt),
    }

    #[async_trait]
    impl ClientCmd for TopicCmd {
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
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
    }

    impl TopicCmd {
        pub fn metadata() -> FluvioExtensionMetadata {
            FluvioExtensionMetadata {
                title: "topic".into(),
                package: Some("fluvio/fluvio".parse().unwrap()),
                description: "Topic Operations".into(),
                version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            }
        }
    }
}
