use std::sync::Arc;
use clap::Parser;
use fluvio::Fluvio;

mod list;

use crate::Result;
use crate::common::output::Terminal;
use crate::common::FluvioExtensionMetadata;
use self::list::ListPartitionOpt;

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

impl PartitionCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }

        Ok(())
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            title: "partition".into(),
            package: Some("fluvio/fluvio".parse().unwrap()),
            description: "Partition Operations".into(),
            version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        }
    }
}
