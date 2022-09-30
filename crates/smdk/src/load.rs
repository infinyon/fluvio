use clap::Parser;
use anyhow::Result;

use crate::wasm::WasmOption;

/// Load SmartModule into Fluvio cluster
#[derive(Debug, Parser)]
pub struct LoadOpt {
    #[clap(long)]
    name: Option<String>,

    #[clap(flatten)]
    wasm: WasmOption,
}
impl LoadOpt {
    pub(crate) fn process(&self) -> Result<()> {
        //println!("loading SmartModule: {}", self.name);
        Ok(())
    }
}
