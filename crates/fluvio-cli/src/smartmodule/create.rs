use std::path::PathBuf;
use structopt::StructOpt;
use crate::Result;

/// Create a new SmartModule with a given name
#[derive(Debug, StructOpt)]
pub struct CreateSmartModuleOpt {
    /// The name of the SmartModule to create
    name: String,
    /// The path to a WASM binary to create the SmartModule from
    #[structopt(long)]
    wasm_file: Option<PathBuf>,
    /// The path to the source code for the SmartModule WASM
    #[structopt(long)]
    source_file: Option<PathBuf>,
}

impl CreateSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
