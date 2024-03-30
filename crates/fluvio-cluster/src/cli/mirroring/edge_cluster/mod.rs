mod status;

use std::sync::Arc;
use clap::Parser;
use anyhow::Result;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use crate::cli::{start::StartOpt, VERSION};

use self::status::StatusOpt;

#[derive(Debug, Parser)]
pub enum EdgeClusterCmd {
    /// Connect will to an Edge cluster, starting a new cluster if necessary
    #[command(name = "connect")]
    Connect(Box<StartOpt>),

    /// Status of the Edge cluster
    #[command(name = "status")]
    Status(StatusOpt),
}

impl EdgeClusterCmd {
    pub async fn execute<O: Terminal>(
        self,
        out: Arc<O>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        match self {
            Self::Connect(connect) => {
                let version = semver::Version::parse(VERSION).unwrap();
                connect.process(version, false).await
            }
            Self::Status(status) => status.execute(out, cluster_target).await,
        }
    }
}
