use anyhow::Result;
use clap::Parser;
use tracing::info;

use cloud_sc_extra::req::RemoteDelete;

use super::common::*;
// use crate::login::CloudClient;

#[derive(Clone, Debug, Parser)]
pub struct DeleteOpt {
    pub name: String,
}
impl DeleteOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let req = RemoteDelete {
            name: self.name.clone(),
        };
        info!(req=?req, "remote-cluster delete request");
        let resp = send_request(cluster_target, req).await?;
        info!("remote cluster delete resp: {}", resp.name);
        Ok(())
    }
}
