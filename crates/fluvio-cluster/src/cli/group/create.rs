//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::spg::*;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser, Default)]
pub struct CreateManagedSpuGroupOpt {
    /// The name for the new SPU Group
    #[clap(value_name = "name")]
    pub name: String,

    /// The number of SPUs to create in this SPG
    #[clap(short, long, value_name = "integer", default_value = "1")]
    pub replicas: u16,

    /// Minimum SPU ID
    #[clap(long, value_name = "integer", default_value = "1")]
    pub min_id: i32,

    /// Rack name
    #[clap(long, value_name = "string")]
    pub rack: Option<String>,

    /// The amount of storage to assign to this SPG
    #[clap(long, value_name = "string")]
    pub storage_size: Option<String>,
}

impl CreateManagedSpuGroupOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let (name, spec) = self.validate();
        debug!("creating spg: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;
        admin.create(name, false, spec).await?;

        Ok(())
    }

    /// Validate cli options. Generate target-server and create spu group config.
    fn validate(self) -> (String, SpuGroupSpec) {
        let storage = self.storage_size.map(|storage_size| StorageConfig {
            size: Some(storage_size),
            ..Default::default()
        });

        let spu_config = SpuConfig {
            storage,
            rack: self.rack,
            ..Default::default()
        };

        let spec = SpuGroupSpec {
            replicas: self.replicas,
            min_id: self.min_id,
            spu_config,
        };
        (self.name, spec)
    }
}
