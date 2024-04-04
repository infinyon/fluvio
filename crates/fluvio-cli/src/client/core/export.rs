use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use fluvio::config::{TlsConfig, TlsPolicy};
use fluvio_controlplane_metadata::upstream::{ClientTls, UpstreamSpec, UpstreamTarget};
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use k8_types::K8Obj;

#[derive(Debug, Parser)]
pub struct ExportOpt {
    // /// Edge name
    edge: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,
    /// override endpoint of the main cluster
    #[arg(long, short = 'e')]
    upstream: Option<String>,
}

impl ExportOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let fluvio_config = cluster_target.load()?;
        let endpoint = fluvio_config.endpoint;

        let metadata_name = format!("edge-{}", self.edge);

        let tls = match fluvio_config.tls {
            TlsPolicy::Verified(TlsConfig::Inline(config)) => Some(ClientTls {
                domain: config.domain.clone(),
                ca_cert: config.ca_cert.clone(),
                client_cert: config.cert.clone(),
                client_key: config.key.clone(),
            }),
            TlsPolicy::Verified(TlsConfig::Files(file_config)) => Some(ClientTls {
                domain: file_config.domain.clone(),
                ca_cert: std::fs::read_to_string(file_config.ca_cert)?,
                client_cert: std::fs::read_to_string(file_config.cert)?,
                client_key: std::fs::read_to_string(file_config.key)?,
            }),
            _ => None,
        };

        let upstream_clusters = vec![K8Obj::new(
            metadata_name,
            UpstreamSpec {
                target: UpstreamTarget { endpoint, tls },
                source_id: self.edge,
                ..Default::default()
            },
        )];

        let metadata = fluvio_sc_schema::edge::EdgeMetadataExport::new(upstream_clusters);

        if let Some(filename) = self.file {
            std::fs::write(filename, serde_json::to_string_pretty(&metadata)?)
                .context("Failed to write output file")?;
        } else {
            out.println(&serde_json::to_string_pretty(&metadata)?);
        }

        Ok(())
    }
}
