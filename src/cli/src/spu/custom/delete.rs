//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use sc_api::spu::FlvCustomSpu;

use crate::error::CliError;
use crate::profile::ScConfig;


// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteCustomSpuOpt {
    /// SPU id
    #[structopt(short = "i", long = "id", required_unless = "name")]
    id: Option<i32>,

    /// SPU name
    #[structopt(
        short = "n",
        long = "name",
        value_name = "string",
        conflicts_with = "id"
    )]
    name: Option<String>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}




impl DeleteCustomSpuOpt {


    /// Validate cli options. Generate target-server and delete custom spu config.
    fn validate(self) -> Result<(ScConfig, FlvCustomSpu), CliError> {

        let target_server = ScConfig::new(self.sc, self.profile)?;

        // custom spu
        let custom_spu = if let Some(name) = self.name {
            FlvCustomSpu::Name(name)
        } else if let Some(id) = self.id {
            FlvCustomSpu::Id(id)
        } else {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                "missing custom SPU name or id",
            )));
        };

        // return server separately from config
        Ok((target_server, custom_spu))
    }


}



// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub async fn process_delete_custom_spu(opt: DeleteCustomSpuOpt) -> Result<(), CliError> {

    let (target_server, cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    sc.delete_custom_spu(cfg).await.map_err(|err| err.into())
}