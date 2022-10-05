use clap::Parser;
use anyhow::Result;

use fluvio::FluvioConfig;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec, SmartModuleMetadata};
use fluvio_extension_common::target::ClusterTarget;
use fluvio::Fluvio;
use fluvio_future::task::run_block_on;

use crate::wasm::WasmOption;

/// Load SmartModule into Fluvio cluster
#[derive(Debug, Parser)]
pub struct LoadOpt {
    #[clap(long)]
    name: Option<String>,

    #[clap(flatten)]
    wasm: WasmOption,

    #[clap(flatten)]
    target: ClusterTarget,
}
impl LoadOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let pkg_metadata = SmartModuleMetadata::from_toml("./SmartModule.toml")?;
        println!("Using SmartModule package: {}", pkg_metadata.package.name);

        let sm_id = pkg_metadata.id();
        let raw_bytes = self.wasm.load_raw_wasm_file()?;
        let spec = SmartModuleSpec {
            meta: Some(pkg_metadata),
            wasm: SmartModuleWasm::from_raw_wasm_bytes(&raw_bytes)?,
        };

        let fluvio_config = self.target.clone().load()?;
        if let Err(e) = run_block_on(self.create(fluvio_config, spec, sm_id)) {
            eprintln!("{}", e);
            std::process::exit(1);
        }

        Ok(())
    }

    async fn create(&self, config: FluvioConfig, spec: SmartModuleSpec, id: String) -> Result<()> {
        println!("trying connectiong to fluvio {}", config.endpoint);
        let fluvio = Fluvio::connect_with_config(&config).await?;

        let admin = fluvio.admin().await;
        println!("creating smartmodule: {}", id);
        admin.create(id, false, spec).await?;

        Ok(())
    }
}
