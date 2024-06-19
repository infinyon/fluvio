use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use fluvio_controlplane_metadata::mirror::{MirrorSpec, MirrorType};
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use fluvio_sc_schema::mirror::Remote;

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
        let fluvio_config = cluster_target.load()?;
        let admin = fluvio::Fluvio::connect_with_config(&fluvio_config)
            .await?
            .admin()
            .await;

        let spec = MirrorSpec {
            mirror_type: MirrorType::Remote(Remote {
                id: self.name.clone(),
            }),
        };

        admin.create(self.name.clone(), false, spec).await?;
        println!("remote cluster \"{}\" was registered", self.name);
        Ok(())
    }
}
