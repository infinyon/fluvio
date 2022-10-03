use clap::Parser;
use anyhow::Result;

use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec, SmartModuleMetadata};

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
        let _pkg_metadata = SmartModuleMetadata::from_toml("./SmartModule.toml")?;

        let raw_bytes = self.wasm.load_raw_wasm_file()?;
        let _spec = SmartModuleSpec {
            wasm: SmartModuleWasm::from_raw_wasm_bytes(&raw_bytes)?,
            ..Default::default()
        };

        Ok(())
    }
}
