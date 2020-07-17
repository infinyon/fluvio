//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use log::debug;
use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_client::metadata::spg::*;

use crate::error::CliError;
use crate::target::ClusterTarget;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short, long, value_name = "string")]
    pub name: String,

    /// SPU replicas
    #[structopt(short, long, value_name = "integer", default_value = "1")]
    pub replicas: u16,

    /// Minimum SPU id (default: 1)
    #[structopt(long, default_value = "1")]
    pub min_id: i32,

    /// Rack name
    #[structopt(long, value_name = "string")]
    pub rack: Option<String>,

    /// storage size
    #[structopt(long, value_name = "string")]
    pub storage_size: Option<String>,

    #[structopt(flatten)]
    pub target: ClusterTarget,
}

impl CreateManagedSpuGroupOpt {
    /// Validate cli options. Generate target-server and create spu group config.
    fn validate(self) -> Result<(ClusterConfig, (String, SpuGroupSpec)), CliError> {
        let target_server = self.target.load()?;

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
        // return server separately from config

        Ok((target_server, group))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------
pub async fn process_create_managed_spu_group(
    opt: CreateManagedSpuGroupOpt,
) -> Result<(), CliError> {
    let (target_server, (name, spec)) = opt.validate()?;

    debug!("creating spg: {}, spec: {:#?}", name, spec);

    let mut target = target_server.connect().await?;

    let mut admin = target.admin().await;

    admin.create(name, false, spec).await?;

    Ok(())
}
