use anyhow::{anyhow, Result};
use clap::Parser;
use tracing::info;

use cloud_sc_extra::req::RemoteRegister;

use crate::cli::remote_cluster::send_request;

use super::common::*;

// example: `fluvio cloud remote-cluster register --type mirror-edge boat1`

#[derive(Debug, Parser)]
pub struct RegisterOpt {
    label: String,

    #[arg(name = "type", long, required = true)]
    kind: String,
}

impl RegisterOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let rs_type = match self.kind.as_str() {
            "mirror-edge" => "mirror-edge".to_string(),
            _ => {
                println!("Allowed values for --type are 'mirror-edge'");
                return Err(anyhow!("invalid remote cluster type {}", self.kind));
            }
        };
        let name = self.label.clone();
        let req = RemoteRegister { name, rs_type };
        info!(req=?req, "remote-cluster register request");
        let resp = send_request(cluster_target, req).await?;
        info!("remote cluster register resp: {}", resp.name);
        Ok(())
    }
}
