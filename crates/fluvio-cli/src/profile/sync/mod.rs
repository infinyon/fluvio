use structopt::StructOpt;

#[cfg(feature = "k8s")]
mod k8;
mod local;

use crate::Result;
use crate::common::COMMAND_TEMPLATE;
use crate::profile::sync::local::LocalOpt;
#[cfg(feature = "k8s")]
use crate::profile::sync::k8::K8Opt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "sync",
    template = COMMAND_TEMPLATE,
)]
pub enum SyncCmd {
    /// Sync a profile from a Kubernetes cluster
    #[cfg(feature = "k8s")]
    #[structopt(name = "k8")]
    K8(K8Opt),
    /// Sync a profile from a local cluster
    #[structopt(name = "local")]
    Local(LocalOpt),
}

impl SyncCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            #[cfg(feature = "k8s")]
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
