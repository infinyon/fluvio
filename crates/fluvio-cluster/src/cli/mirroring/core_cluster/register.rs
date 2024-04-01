use anyhow::Result;
use clap::Parser;
use fluvio_controlplane_metadata::remote_cluster::{KeyPair, RemoteClusterSpec, RemoteClusterType};
use super::{common::*, get_admin};

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
        let admin = get_admin(cluster_target).await?;

        let spec = RemoteClusterSpec {
            remote_type: RemoteClusterType::MirrorEdge,
            key_pair: KeyPair {
                public_key: "".into(),
                private_key: "".into(),
            },
        };
        admin.create(name, false, spec).await?;
        Ok(())
    }
}
