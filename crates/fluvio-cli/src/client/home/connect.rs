use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use clap::Parser;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::Terminal;
use fluvio_sc_schema::mirror::{MirrorSpec, MirrorType};
use fluvio_sc_schema::remote_file::RemoteMetadataExport;

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
        let remote_metadata: RemoteMetadataExport = serde_json::from_reader(reader)
            .map_err(|err| anyhow!("unable to load remote metadata: {}", err))?;
        let home = remote_metadata.home;

        let home_id = home.id.clone();

        let spec = MirrorSpec {
            mirror_type: MirrorType::Home(home),
        };

        admin.create(home_id.clone(), false, spec).await?;
        println!("connecting with \"{}\" cluster", home_id);
        Ok(())
    }
}
