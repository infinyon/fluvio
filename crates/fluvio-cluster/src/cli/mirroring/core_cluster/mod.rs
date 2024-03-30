// So the idea here is for remote-cluster commands
// open an admin socket, start with a remote-cluster "start" command
// which should switch a remote-cluster specific interface
// after which specific commands can be sent
// might need to refactor towards it later

pub mod unregister;
pub mod list;
pub mod register;
pub mod export;

use anyhow::Result;
use clap::Parser;

use unregister::UnregisterOpt;
use list::ListOpt;
use register::RegisterOpt;

mod common {
    pub use std::sync::Arc;

    pub use fluvio_extension_common::target::ClusterTarget;
    pub use fluvio_extension_common::output::Terminal;

    // pub use super::get_admin;
    // pub use super::send_request;
}
use common::*;

#[derive(Debug, Parser)]
pub enum CoreClusterCmd {
    /// Register a new remote cluster
    #[command(
        name = "register",
        long_about = "Register a remote cluster\n\nExample: fluvio cloud remote-cluster register --type mirror-edge boat1"
    )]
    Register(RegisterOpt),

    /// List all remote clusters
    #[command(name = "list")]
    List(ListOpt),

    /// List all remote clusters
    #[command(name = "unregister")]
    Unregister(UnregisterOpt),

    /// Generate metadata file for remote cluster
    #[command(name = "export")]
    Export(ExportOpt),
}

impl CoreClusterCmd {
    pub async fn execute<O: Terminal>(
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

use fluvio::FluvioAdmin;
pub async fn get_admin(cluster_target: ClusterTarget) -> Result<FluvioAdmin> {
    let fluvio_config = cluster_target.load()?;
    // let config_file = fluvio::config::ConfigFile::load_default_or_new()?;
    let flv = fluvio::Fluvio::connect_with_config(&fluvio_config).await?;
    let admin = flv.admin().await;
    Ok(admin)
}

// use fluvio_sc_schema::core::Spec;
// use cloud_sc_extra::{CloudRemoteClusterSpec, remote::CloudRemoteClusterRequest, CloudStatus};

use self::export::ExportOpt;
// pub async fn send_request<R: Spec + CloudRemoteClusterSpec>(
//     cluster_target: ClusterTarget,
//     req: R,
// ) -> Result<CloudStatus> {
//     use fluvio_sc_schema::cloud::ObjectCloudRequest;
//     let req = CloudRemoteClusterRequest { request: req };
//     let admin = get_admin(cluster_target).await?;
//     let resp = admin
//         .send_receive_admin::<ObjectCloudRequest, _>(req)
//         .await?;
//     debug!(resp=?resp, "Response");
//     Ok(resp)
// }
