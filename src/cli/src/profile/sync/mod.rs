use structopt::StructOpt;

mod k8;
mod local;

use crate::Result;
use crate::profile::sync::local::LocalOpt;
use crate::profile::sync::k8::K8Opt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sync",
    template = crate::COMMAND_TEMPLATE,
)]
pub enum SyncCmd {
    #[structopt(name = "k8", about = "sync profile from kubernetes cluster")]
    K8(K8Opt),
    #[structopt(name = "local", about = "sync profile from local cluster")]
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
