pub mod status;
pub mod connect;

use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use fluvio::FluvioAdmin;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::output::Terminal;

use self::connect::ConnectOpt;
use self::status::StatusOpt;

#[derive(Debug, Parser)]
pub enum HomeCmd {
    /// Connect to a home cluster
    #[command(name = "connect")]
    Connect(ConnectOpt),
    /// Get the status of a home cluster
    #[command(name = "status")]
    Status(StatusOpt),
}

impl HomeCmd {
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        match self {
            Self::Connect(conn) => conn.execute(out, cluster_target).await,
            Self::Status(status) => status.execute(out, cluster_target).await,
        }
    }
}

pub async fn get_admin(cluster_target: ClusterTarget) -> Result<FluvioAdmin> {
    let fluvio_config = cluster_target.load()?;
    let flv = fluvio::Fluvio::connect_with_config(&fluvio_config).await?;
    let admin = flv.admin().await;
    Ok(admin)
}
