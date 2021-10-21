use std::path::PathBuf;
use structopt::StructOpt;

use fluvio::Fluvio;

use crate::Result;

/// Create a new SmartModule with a given name
#[derive(Debug, StructOpt)]
pub struct CreateSmartStreamOpt {
    /// The name of the SmartModule to create
    name: String,
}

impl CreateSmartStreamOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        todo!()
    }
}
