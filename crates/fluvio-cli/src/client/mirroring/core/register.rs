use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use fluvio_controlplane_metadata::remote::{KeyPair, RemoteSpec, RemoteType};
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use super::get_admin;

#[derive(Debug, Parser)]
pub struct RegisterOpt {
    name: String,
}

impl RegisterOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let admin = get_admin(cluster_target).await?;

        let spec = RemoteSpec {
            remote_type: RemoteType::Edge,
            key_pair: KeyPair {
                public_key: "".into(),
                private_key: "".into(),
            },
        };
        admin.create(self.name.clone(), false, spec).await?;

        println!("Edge cluster {} was registered", self.name);
        Ok(())
    }
}
