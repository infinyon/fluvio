use anyhow::Result;
use clap::Parser;
use tracing::info;

use cloud_sc_extra::remote::RemoteRegister;

use crate::cli::mirroring::core_cluster::send_request;

use super::common::*;

const RS_TYPE: &str = "mirror-edge";

// example: `fluvio cloud remote-cluster register --type mirror-edge boat1`

#[derive(Debug, Parser)]
pub struct RegisterOpt {
    label: String,
}

impl RegisterOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let name = self.label.clone();
        let rs_type = RS_TYPE.to_owned();
        let req = RemoteRegister { name, rs_type };
        info!(req=?req, "remote-cluster register request");
        let resp = send_request(cluster_target, req).await?;
        info!("remote cluster register resp: {}", resp.name);
        println!("Edge cluster {} was registered", self.label);
        Ok(())
    }
}
