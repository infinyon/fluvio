use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use fluvio_sc_schema::{
    edge::EdgeMetadataExport,
    remote::{Core, RemoteSpec, RemoteType},
};
use k8_types::K8Obj;

#[derive(Debug, Parser)]
pub struct ExportOpt {
    /// id of the remote cluster
    remote_id: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,
    /// override endpoint of the core cluster
    #[arg(long, short = 'e')]
    public_endpoint: Option<String>,
}

impl ExportOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let fluvio_config = cluster_target.load()?;

        let public_endpoint = if let Some(public_endpoint) = self.public_endpoint {
            public_endpoint
        } else {
            fluvio_config.endpoint
        };

        let metadata_name = format!("edge-{}", self.remote_id);
        let edge_metadata = vec![K8Obj::new(
            metadata_name,
            RemoteSpec {
                remote_type: RemoteType::Core(Core {
                    id: self.remote_id,
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
