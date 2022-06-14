use tracing::debug;
use clap::Parser;
use duct::{cmd};

use fluvio::Fluvio;

use crate::Result;
use super::fluvio_smart_dir;

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct TestSmartModuleOpt {
    /// The name of the SmartModule to generate
    name: String,
}

impl TestSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        let sm_base_dir = fluvio_smart_dir()?;
        let sm_dir = sm_base_dir.join(&self.name);
        if !sm_dir.exists() {
            println!("SmartModule {} does not exist", self.name);
            return Ok(());
        }

        // generate a simple template
        cmd!(
            "cargo",
            "build",
            "--release",
            "--target",
            "wasm32-unknown-unknown"
        )
        .dir(&sm_dir)
        .run()?;

        // get wasm file directory
        let wasm_file = sm_dir
            .join("target")
            .join("wasm32-unknown-unknown")
            .join("release")
            .join(format!("{}.wasm", self.name));
        debug!(?wasm_file, "wasm file");
        let raw = std::fs::read(wasm_file)?;
        println!("loading {} wasm bytes", raw.len());

        Ok(())
    }
}
