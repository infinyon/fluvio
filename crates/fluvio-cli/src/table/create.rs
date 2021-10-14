//!
//! # Create a Table display specification
//!
//! CLI tree to generate Create a Table spec
//!

use structopt::StructOpt;
//use tracing::debug;

use fluvio::Fluvio;
//use fluvio_controlplane_metadata::table::TableSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateTableOpt {
    /// The name for the new Table spec
    #[structopt(short = "c", long = "config", value_name = "config")]
    pub config: String,
}

impl CreateTableOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<(), CliError> {
        //let spec: Table = config.clone().into();
        //let name = spec.name.clone();

        //debug!("creating table spec: {}, spec: {:#?}", name, spec);

        Ok(())
    }
}
