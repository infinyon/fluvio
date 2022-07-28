use std::path::PathBuf;
use tracing::debug;
use clap::Parser;
use crate::Result;
use fluvio::Fluvio;
use fluvio::metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec};
use flate2::{Compression, bufread::GzEncoder};
use std::io::Read;

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct CreateSmartModuleOpt {
    /// The name of the SmartModule to create
    name: String,
    /// The path to a WASM binary to create the SmartModule from
    #[clap(long)]
    wasm_file: PathBuf,
    /// The path to the source code for the SmartModule WASM
    #[clap(long)]
    _source_file: Option<PathBuf>,
}

impl CreateSmartModuleOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let raw = std::fs::read(self.wasm_file)?;
        let mut encoder = GzEncoder::new(raw.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw.len());
        encoder.read_to_end(&mut buffer)?;
        /*
         * TODO: Fix the CRD to work with this
        let buffer = vec!['a' as u8; self.size];
        */

        let spec: SmartModuleSpec = SmartModuleSpec {
            wasm: SmartModuleWasm::from_binary_payload(buffer),
            ..Default::default()
        };
        
        debug!(name = self.name, "creating smart-module");
        let admin = fluvio.admin().await;
        admin.create(self.name.to_string(), false, spec).await?;
        println!("smart-module \"{}\" has been created.", self.name);

        Ok(())
    }
}
