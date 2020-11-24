mod group;
mod spu;
mod install;
mod uninstall;
mod util;
mod check;
mod releases;
mod error;

pub use self::error::ClusterCmdError;
use self::error::Result;
use fluvio_extension_common as common;

pub use opt::ClusterCmd;
mod opt {

    use std::sync::Arc;

    use structopt::StructOpt;

    use fluvio_runner_local::run::RunnerCmd;

    use crate::extension::common::target::ClusterTarget;
    use crate::extension::common::output::Terminal;

    use super::install::InstallOpt;
    use super::uninstall::UninstallOpt;
    use super::check::CheckOpt;
    use super::releases::ReleasesCmd;
    use super::group::SpuGroupCmd;
    use super::spu::SpuCmd;

    use crate::extension::Result;

    /// Cluster commands
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

        /// Manage and view Streaming Processing Units (SPUs)
        ///
        /// SPUs make up the part of a Fluvio cluster which is in charge
        /// of receiving messages from producers, storing those messages,
        /// and relaying them to consumers. This command lets you see
        /// the status of SPUs in your cluster.
        #[structopt(name = "spu")]
        SPU(SpuCmd),

        /// Manage and view SPU Groups (SPGs)
        ///
        /// SPGs are groups of SPUs in a cluster which are managed together.
        #[structopt(name = "spg")]
        SPUGroup(SpuGroupCmd),

        /// Run a Streaming Controller (SC) or SPU
        #[structopt(flatten)]
        Run(RunnerCmd),
    }

    impl ClusterCmd {
        /// process cluster commands
        pub async fn process<O: Terminal>(self, out: Arc<O>, target: ClusterTarget) -> Result<()> {
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
                Self::SPU(spu) => {
                    let fluvio = target.connect().await?;
                    spu.process(out, &fluvio).await?;
                }
                Self::SPUGroup(group) => {
                    let fluvio = target.connect().await?;
                    group.process(out, &fluvio).await?;
                }
                Self::Run(run) => {
                    run.process().await?;
                }
            }

            Ok(())
        }
    }
}
