use structopt::StructOpt;

mod k8;
mod local;

use crate::Result;
use crate::common::COMMAND_TEMPLATE;
use crate::profile::sync::local::LocalOpt;
use crate::profile::sync::k8::K8Opt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sync",
    template = COMMAND_TEMPLATE,
)]
pub enum SyncCmd {
    /// Sync a profile from a Kubernetes cluster
    #[structopt(name = "k8")]
    K8(K8Opt),
    /// Sync a profile from a local cluster
    #[structopt(name = "local")]
    Local(LocalOpt),
}

impl SyncCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::K8(k8) => {
                k8.process().await?;
            }
            Self::Local(local) => {
                local.process().await?;
            }
        }
        Ok(())
    }
}
