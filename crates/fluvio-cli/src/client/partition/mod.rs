mod list;

pub use cmd::PartitionCmd;

mod cmd {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;
    use anyhow::Result;

    use fluvio::Fluvio;

    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;
    use crate::common::FluvioExtensionMetadata;

    use super::list::ListPartitionOpt;

    #[derive(Debug, Parser)]
    #[clap(name = "partition", about = "Partition operations")]
    pub enum PartitionCmd {
        /// List all of the Partitions in this cluster
        #[clap(
            name = "list",
            help_template = crate::common::COMMAND_TEMPLATE,
        )]
        List(ListPartitionOpt),
    }

    #[async_trait]
    impl ClientCmd for PartitionCmd {
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
            match self {
                Self::List(list) => {
                    list.process(out, fluvio).await?;
                }
            }

            Ok(())
        }
    }

    impl PartitionCmd {
        pub fn metadata() -> FluvioExtensionMetadata {
            FluvioExtensionMetadata {
                title: "partition".into(),
                package: Some("fluvio/fluvio".parse().unwrap()),
                description: "Partition Operations".into(),
                version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            }
        }
    }
}
