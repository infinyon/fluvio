use std::sync::Arc;
use structopt::StructOpt;

mod group;
mod spu;
mod start;
mod delete;
mod util;
mod check;
mod releases;
mod error;

use start::StartOpt;
use start::UpgradeOpt;
use delete::DeleteOpt;
use check::CheckOpt;
use releases::ReleasesCmd;
use group::SpuGroupCmd;
use spu::SpuCmd;

pub use self::error::ClusterCliError;

use fluvio_extension_common as common;
use common::target::ClusterTarget;
use common::output::Terminal;

/// Manage and view Fluvio clusters
#[derive(StructOpt, Debug)]
pub enum ClusterCmd {
    /// Start a Fluvio cluster, locally or on Minikube
    #[structopt(name = "start")]
    Start(Box<StartOpt>),

    /// Upgrades an already-started Fluvio cluster
    #[structopt(name = "upgrade")]
    Upgrade(Box<UpgradeOpt>),

    /// Delete a Fluvio cluster from the local machine or Minikube
    #[structopt(name = "delete")]
    Delete(DeleteOpt),

    /// Check that all requirements for cluster startup are met
    #[structopt(name = "check")]
    Check(CheckOpt),

    /// Print information about various Fluvio releases
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
}

impl ClusterCmd {
    /// process cluster commands
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        default_chart_version: &str,
        target: ClusterTarget,
    ) -> Result<(), ClusterCliError> {
        match self {
            Self::Start(start) => {
                start.process(default_chart_version, false, false).await?;
            }
            Self::Upgrade(upgrade) => {
                upgrade.process(default_chart_version).await?;
            }
            Self::Delete(uninstall) => {
                uninstall.process().await?;
            }
            Self::Check(check) => {
                check.process(default_chart_version).await?;
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
        }

        Ok(())
    }
}
