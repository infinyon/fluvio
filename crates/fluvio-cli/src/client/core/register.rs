use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use fluvio_controlplane_metadata::remote::{RemoteSpec, RemoteType};
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use fluvio_sc_schema::remote::Core;

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
        let public_endpoint = fluvio_config.endpoint;

        let spec = RemoteSpec {
            remote_type: RemoteType::Core(Core {
                id: self.name.clone(),
                public_endpoint,
            }),
        };
        admin.create(self.name.clone(), false, spec).await?;
        println!("edge cluster {:?} was registered", self.name);
        Ok(())
    }
}
