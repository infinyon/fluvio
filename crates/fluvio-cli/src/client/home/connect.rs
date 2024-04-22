use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use clap::Parser;
use fluvio_controlplane_metadata::remote::{RemoteSpec, RemoteType};
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use fluvio_sc_schema::edge::EdgeMetadataExport;

#[derive(Debug, Parser)]
pub struct ConnectOpt {
    #[arg(long, short = 'f')]
    file: String,
}

impl ConnectOpt {
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

        let reader = BufReader::new(File::open(self.file)?);
        let edge_metadata: EdgeMetadataExport = serde_json::from_reader(reader)
            .map_err(|err| anyhow!("unable to load edge metadata: {}", err))?;
        let core = edge_metadata.core;

        let core_id = core.id.clone();

        let spec = RemoteSpec {
            remote_type: RemoteType::Core(core),
        };

        admin.create(core_id.clone(), false, spec).await?;
        println!("connecting with \"{}\" cluster", core_id);
        Ok(())
    }
}
