use std::sync::Arc;

use structopt::StructOpt;
use semver::Version;

mod group;
mod spu;
mod start;
mod delete;
mod util;
mod check;
mod error;
mod diagnostics;

pub use start::StartOpt;
pub use start::UpgradeOpt;
use delete::DeleteOpt;
use check::CheckOpt;
use group::SpuGroupCmd;
use spu::SpuCmd;
use diagnostics::DiagnosticsOpt;

pub use self::error::ClusterCliError;

use fluvio_extension_common as common;
use common::target::ClusterTarget;
use common::output::Terminal;

/// Manage and view Fluvio clusters
#[derive(StructOpt, Debug, Clone)]
pub enum ClusterCmd {
    /// Install Fluvio cluster
    #[structopt(name = "start")]
    Start(Box<StartOpt>),

    /// Upgrades an already-started Fluvio cluster
    #[structopt(name = "upgrade")]
    Upgrade(Box<UpgradeOpt>),

    /// Uninstall a Fluvio cluster
    #[structopt(name = "delete")]
    Delete(DeleteOpt),

    /// Check that all requirements for cluster startup are met
    #[structopt(name = "check")]
    Check(CheckOpt),

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

    /// Collect anonymous diagnostic information to help with debugging
    #[structopt(name = "diagnostics")]
    Diagnostics(DiagnosticsOpt),
}

impl ClusterCmd {
    /// process cluster commands
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        platform_version: Version,
        target: ClusterTarget,
    ) -> Result<(), ClusterCliError> {
        match self {
            Self::Start(start) => {
                start.process(platform_version, false, false).await?;
            }
            Self::Upgrade(upgrade) => {
                upgrade.process(platform_version).await?;
            }
            Self::Delete(uninstall) => {
                uninstall.process().await?;
            }
            Self::Check(check) => {
                check.process(platform_version).await?;
            }
            Self::SPU(spu) => {
                let fluvio = target.connect().await?;
                spu.process(out, &fluvio).await?;
            }
            Self::SPUGroup(group) => {
                let fluvio = target.connect().await?;
                group.process(out, &fluvio).await?;
            }
            Self::Diagnostics(opt) => {
                opt.process().await?;
            }
        }

        Ok(())
    }
}
