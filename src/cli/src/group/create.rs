//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::spg::*;
use crate::Result;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateManagedSpuGroupOpt {
    /// The name for the new SPU Group
    #[structopt(value_name = "name")]
    pub name: String,

    /// The number of SPUs to create in this SPG
    #[structopt(short, long, value_name = "integer", default_value = "1")]
    pub replicas: u16,

    /// Minimum SPU ID
    #[structopt(long, value_name = "integer", default_value = "1")]
    pub min_id: i32,

    /// Rack name
    #[structopt(long, value_name = "string")]
    pub rack: Option<String>,

    /// The amount of storage to assign to this SPG
    #[structopt(long, value_name = "string")]
    pub storage_size: Option<String>,
}

impl CreateManagedSpuGroupOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let (name, spec) = self.validate()?;
        debug!("creating spg: {}, spec: {:#?}", name, spec);

        let mut admin = fluvio.admin().await;
        admin.create(name, false, spec).await?;

        Ok(())
    }

    /// Validate cli options. Generate target-server and create spu group config.
    fn validate(self) -> Result<(String, SpuGroupSpec)> {
        let storage = self.storage_size.map(|storage_size| StorageConfig {
            size: Some(storage_size),
            ..Default::default()
        });

        let spu_config = SpuConfig {
            storage,
            rack: self.rack,
            ..Default::default()
        };
        let group = (
            self.name,
            SpuGroupSpec {
                replicas: self.replicas,
                min_id: self.min_id,
                spu_config,
            },
        );

        Ok(group)
    }
}
