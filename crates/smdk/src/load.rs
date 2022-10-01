use clap::Parser;
use anyhow::Result;

use fluvio::metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec};

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

        /* 
        let mut encoder = GzEncoder::new(raw.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw.len());
        encoder.read_to_end(&mut buffer)?;

        let spec: SmartModuleSpec = SmartModuleSpec {
            wasm: SmartModuleWasm::from_binary_payload(buffer),
            package: package_opt.0,
            init_params: package_opt.1,
            ..Default::default()
        };
        let wasm = SmartModuleWasm::new(self.wasm.wasm_file.clone())?;
        */
        
        Ok(())
    }
}
