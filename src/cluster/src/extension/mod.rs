mod cluster;
mod group;
mod spu;
mod install;
mod uninstall;
mod util;
mod check;
mod releases;
mod error;

use self::error::{ ClusterCmdError, Result};
use fluvio_extension_common as common;


pub use opt::ClusterCmd;
mod opt {

    use structopt::StructOpt;

    use super::install::InstallOpt;
    use super::uninstall::UninstallOpt;
    use super::check::CheckOpt;
    use super::releases::ReleasesCmd;

    use crate::extension::Result;

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
}