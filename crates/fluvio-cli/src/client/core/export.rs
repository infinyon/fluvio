use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use fluvio_sc_schema::{
    edge::EdgeMetadataExport,
    remote::{Core, RemoteSpec, RemoteType},
};
use k8_types::K8Obj;
use anyhow::anyhow;

use super::get_admin;

#[derive(Debug, Parser)]
pub struct ExportOpt {
    /// id of the edge cluster to export
    edge_id: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,
    /// override endpoint of the core cluster
    #[arg(long, short = 'e')]
    public_endpoint: Option<String>,
    // id of the core cluster to share
    #[arg(name = "c")]
    core_id: Option<String>,
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
        let all_remotes = admin.all::<RemoteSpec>().await?;
        let _edge = all_remotes
            .iter()
            .find(|remote| match &remote.spec.remote_type {
                RemoteType::Edge(edge) => edge.id == self.edge_id,
                _ => false,
            })
            .ok_or_else(|| anyhow!("edge cluster not found"))?;

        let core_id = self.core_id.clone().unwrap_or_else(|| "core".to_owned());

        let metadata_name = format!("edge-{}", self.edge_id);
        let edge_metadata = vec![K8Obj::new(
            metadata_name,
            RemoteSpec {
                remote_type: RemoteType::Core(Core {
                    id: core_id,
                    edge_id: self.edge_id,
                    public_endpoint,
                }),
            },
        )];

        let metadata = EdgeMetadataExport::new(edge_metadata);

        if let Some(filename) = self.file {
            std::fs::write(filename, serde_json::to_string_pretty(&metadata)?)
                .context("failed to write output file")?;
        } else {
            out.println(&serde_json::to_string_pretty(&metadata)?);
        }

        Ok(())
    }
}
