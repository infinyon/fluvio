use clap::Parser;
use duct::{cmd};

use fluvio::Fluvio;

use crate::Result;

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct GenerateSmartModuleOpt {
    /// The name of the SmartModule to generate
    name: String,
}

impl GenerateSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        // generate a simple template
        cmd!("cargo", "generate").run()?;

        Ok(())
    }
}
