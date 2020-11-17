use structopt::StructOpt;

mod install;
mod uninstall;
mod util;
mod check;
mod releases;

use install::InstallOpt;
use uninstall::UninstallOpt;
use check::CheckOpt;
use releases::ReleasesCmd;
use crate::Result;

#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ClusterCmd {
    /// Install a Fluvio cluster, locally or on Minikube
    #[structopt(name = "install")]
    Install(Box<InstallOpt>),

    /// Uninstall a Fluvio cluster from the local machine or Minkube
    #[structopt(name = "uninstall")]
    Uninstall(UninstallOpt),

    /// Check that all requirements for installation are met
    #[structopt(name = "check")]
    Check(CheckOpt),

    /// Prints information about various Fluvio releases
    #[structopt(name = "releases")]
    Releases(ReleasesCmd),
}

impl ClusterCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::Install(install) => {
                install.process().await?;
            }
            Self::Uninstall(uninstall) => {
                uninstall.process().await?;
            }
            Self::Check(check) => {
                check.process().await?;
            }
            Self::Releases(releases) => {
                releases.process().await?;
            }
        }

        Ok(())
    }
}
