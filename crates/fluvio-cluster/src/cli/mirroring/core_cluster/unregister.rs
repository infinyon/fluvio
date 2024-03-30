use anyhow::Result;
use clap::Parser;
use fluvio_sc_schema::remote::RemoteClusterSpec;
use super::{common::*, get_admin};

#[derive(Clone, Debug, Parser)]
pub struct UnregisterOpt {
    pub name: String,
}
impl UnregisterOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        // let req = RemoteDelete {
        // name: self.name.clone(),
        // };
        // info!(req=?req, "remote-cluster delete request");
        // let resp = send_request(cluster_target, req).await?;
        // info!("remote cluster delete resp: {}", resp.name);
        // Ok(())

        let admin = get_admin(cluster_target).await?;
        admin.delete::<RemoteClusterSpec>(&self.name).await?;
        Ok(())
    }
}
