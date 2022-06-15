use std::path::PathBuf;

use tracing::info;
use clap::Parser;
use duct::{cmd};
use include_dir::{Dir, include_dir};

use fluvio::Fluvio;
use fluvio_cli_common::install::fluvio_base_dir;

use crate::Result;
use super::fluvio_smart_dir;

const TEMPLATE_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/templates");

/// return template directory for each CLI version
/// if it doesn't exists, create template directory and unpack from inline template
fn fluvio_template_dir() -> Result<PathBuf> {
    let base_dir = fluvio_base_dir()?;
    let template_path = base_dir.join("templates").join(env!("GIT_HASH"));
    if !template_path.exists() {
        info!(log = ?template_path,"creating smart module template directory");
        std::fs::create_dir_all(&template_path)?;
        TEMPLATE_DIR.extract(&template_path)?;
    }
    Ok(template_path)
}

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct GenerateSmartModuleOpt {
    /// The name of the SmartModule to generate
    name: String,
}

impl GenerateSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        let sm_dir = fluvio_smart_dir()?;
        let template_dir = fluvio_template_dir()?;

        // generate a simple template
        cmd!(
            "cargo",
            "generate",
            "--path",
            template_dir,
            "-n",
            self.name,
            "-d",
            "smartmodule-type=map",
            "-d",
            "smartmodule-params=false"
        )
        .dir(sm_dir)
        .run()?;

        Ok(())
    }
}
