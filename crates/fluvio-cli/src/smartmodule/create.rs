use std::path::PathBuf;
use structopt::StructOpt;
use crate::Result;
use fluvio::Fluvio;
use fluvio::metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec};

/// Create a new SmartModule with a given name
#[derive(Debug, StructOpt)]
pub struct CreateSmartModuleOpt {
    /// The name of the SmartModule to create
    name: String,
    /// The path to a WASM binary to create the SmartModule from
    #[structopt(long)]
    wasm_file: PathBuf,
    /// The path to the source code for the SmartModule WASM
    #[structopt(long)]
    source_file: Option<PathBuf>,
}

impl CreateSmartModuleOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let raw = std::fs::read(self.wasm_file)?;

        let spec: SmartModuleSpec = SmartModuleSpec {
            wasm: Some(SmartModuleWasm::from_binary_payload(raw)),
            ..Default::default()
        };
        let admin = fluvio.admin().await;
        admin.create(self.name.to_string(), false, spec).await?;
        Ok(())
    }
}
