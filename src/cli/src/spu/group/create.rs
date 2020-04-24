//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use log::debug;
use structopt::StructOpt;

use sc_api::spu::FlvCreateSpuGroupRequest;
use flv_client::profile::ScConfig;

use crate::error::CliError;
use crate::tls::TlsConfig;
use super::helpers::group_config::GroupConfig;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: String,

    /// SPU replicas
    #[structopt(short = "l", long = "replicas")]
    replicas: u16,

    /// Minimum SPU id (default: 1)
    #[structopt(short = "i", long = "min-id")]
    min_id: Option<i32>,

    /// Rack name
    #[structopt(short = "r", long = "rack", value_name = "string")]
    rack: Option<String>,

    /// storage size
    #[structopt(short = "s", long = "size", value_name = "string")]
    storage: Option<String>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    #[structopt(flatten)]
    tls: TlsConfig,
}

impl CreateManagedSpuGroupOpt {
    /// Validate cli options. Generate target-server and create spu group config.
    fn validate(self) -> Result<(ScConfig, FlvCreateSpuGroupRequest), CliError> {
        let target_server = ScConfig::new(self.sc,self.tls.try_into_file_config()?)?;

        let grp_config = self
            .storage
            .map(|storage| GroupConfig::with_storage(storage));

        let group = FlvCreateSpuGroupRequest {
            name: self.name,
            replicas: self.replicas,
            min_id: self.min_id,
            config: grp_config.map(|cf| cf.into()).unwrap_or_default(),
            rack: self.rack,
        };
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
    let (target_server, create_spu_group_cfg) = opt.validate()?;

    debug!("{:#?}", create_spu_group_cfg);

    let mut sc = target_server.connect().await?;

    sc.create_group(create_spu_group_cfg)
        .await
        .map_err(|err| err.into())
}
