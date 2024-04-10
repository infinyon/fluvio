use anyhow::Result;
use clap::Parser;
use fluvio_sc_schema::remote::RemoteSpec;
use std::sync::Arc;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use super::get_admin;

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
        let admin = get_admin(cluster_target).await?;
        admin.delete::<RemoteSpec>(&self.name).await?;
        println!("edge cluster {:?} was unregistered", self.name);
        Ok(())
    }
}
