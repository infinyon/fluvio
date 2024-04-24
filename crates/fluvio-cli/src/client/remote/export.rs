use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use fluvio_sc_schema::{
    mirror::{Home, MirrorSpec, MirrorType},
    remote_file::RemoteMetadataExport,
};
use anyhow::anyhow;

use super::get_admin;

#[derive(Debug, Parser)]
pub struct ExportOpt {
    /// id of the remote cluster to export
    remote_id: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,
    /// override endpoint of the home cluster
    #[arg(long, short = 'e')]
    public_endpoint: Option<String>,
    // id of the home cluster to share
    #[arg(name = "c")]
    home_id: Option<String>,
}

impl ExportOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let public_endpoint = if let Some(public_endpoint) = self.public_endpoint {
            public_endpoint
        } else {
            let fluvio_config = cluster_target.clone().load()?;
            fluvio_config.endpoint
        };

        let admin = get_admin(cluster_target).await?;
        let all_remotes = admin.all::<MirrorSpec>().await?;
        let _remote = all_remotes
            .iter()
            .find(|remote| match &remote.spec.mirror_type {
                MirrorType::Remote(remote) => remote.id == self.remote_id,
                _ => false,
            })
            .ok_or_else(|| anyhow!("remote cluster not found"))?;

        let home_id = self.home_id.clone().unwrap_or_else(|| "home".to_owned());

        let home_metadata = Home {
            id: home_id,
            remote_id: self.remote_id,
            public_endpoint,
        };

        let metadata = RemoteMetadataExport::new(home_metadata);

        if let Some(filename) = self.file {
            std::fs::write(filename, serde_json::to_string_pretty(&metadata)?)
                .context("failed to write output file")?;
        } else {
            out.println(&serde_json::to_string_pretty(&metadata)?);
        }

        Ok(())
    }
}
