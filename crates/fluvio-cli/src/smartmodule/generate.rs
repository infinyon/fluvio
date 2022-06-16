use std::path::PathBuf;

use tracing::info;
use clap::{Parser, ArgEnum};
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

#[derive(ArgEnum, Debug, Clone, PartialEq)]
#[allow(non_camel_case_types)]
pub enum DataType {
    json,
}

impl Default for DataType {
    fn default() -> Self {
        DataType::json
    }
}

#[derive(ArgEnum, Debug, Clone, PartialEq)]
#[allow(non_camel_case_types)]
pub enum ModuleType {
    filter,
    map,
}

impl Default for ModuleType {
    fn default() -> Self {
        ModuleType::map
    }
}

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct GenerateSmartModuleOpt {
    /// The name of the SmartModule to generate
    name: String,

    #[clap(default_value_t, value_name = "type", arg_enum, ignore_case = true)]
    ty: ModuleType,

    #[clap(
        default_value_t,
        short = 'i',
        long,
        value_name = "data-type",
        arg_enum,
        ignore_case = true
    )]
    input: DataType,

    #[clap(
        default_value_t,
        short = 'o',
        long,
        value_name = "data-type",
        arg_enum,
        ignore_case = true
    )]
    output: DataType,

    #[clap(long)]
    jolt: Option<String>,
}

impl GenerateSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        if let Some(jolt) = self.jolt {
            let sm_dir = fluvio_smart_dir()?;
            let template_dir = fluvio_template_dir()?.join("jolt-map");

            // generate a simple template
            cmd!(
                "cargo",
                "generate",
                "--path",
                template_dir,
                "-n",
                self.name,
                "-d",
                "smartmodule-params=false",
            )
            .dir(sm_dir)
            .env("CARGO_GENERATE_VALUE_JOLT", jolt)
            .run()?;
        } else {
            println!(
                "no smart module is generated. please use --jolt option to generate a smart module"
            );
        }

        Ok(())
    }
}
