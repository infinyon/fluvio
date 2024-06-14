pub mod unregister;
pub mod list;
pub mod register;
pub mod export;

use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use fluvio_extension_common::target::ClusterTarget;
use unregister::UnregisterOpt;
use list::ListOpt;
use register::RegisterOpt;
use fluvio::FluvioAdmin;
use fluvio_extension_common::output::Terminal;
use self::export::ExportOpt;

#[derive(Debug, Parser)]
pub enum RemoteCmd {
    /// Register a new remote cluster
    #[command(name = "register")]
    Register(RegisterOpt),
    /// List all remote clusters
    #[command(name = "list")]
    List(ListOpt),
    /// Unregister a remote cluster
    #[command(name = "unregister")]
    Unregister(UnregisterOpt),
    /// Generate metadata file for remote cluster
    #[command(name = "export")]
    Export(ExportOpt),
}

impl RemoteCmd {
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        match self {
            Self::Register(reg) => reg.execute(out, cluster_target).await,
            Self::Unregister(del) => del.execute(out, cluster_target).await,
            Self::List(list) => list.execute(out, cluster_target).await,
            Self::Export(meta) => meta.execute(out, cluster_target).await,
        }
    }
}

pub async fn get_admin(cluster_target: ClusterTarget) -> Result<FluvioAdmin> {
    let fluvio_config = cluster_target.load()?;
    let flv = fluvio::Fluvio::connect_with_config(&fluvio_config).await?;
    let admin = flv.admin().await;
    Ok(admin)
}
