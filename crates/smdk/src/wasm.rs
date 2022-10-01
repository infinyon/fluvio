use std::path::PathBuf;
use std::fmt::Debug;

use cargo_metadata::{MetadataCommand, CargoOpt};
use clap::Parser;
use anyhow::Result;
use convert_case::{Case, Casing};

#[derive(Debug, Parser)]
pub struct WasmOption {
    /// release name
    #[clap(long, default_value = "release-lto")]
    release: String,

    /// optional wasm_file path
    #[clap(long)]
    wasm_file: Option<PathBuf>,
}

impl WasmOption {
    pub(crate) fn load_raw_wasm_file(&self) -> Result<Vec<u8>> {
        let wasm_path = self.wasm_file_path()?;
        println!("loading module at: {}", wasm_path.display());
        std::fs::read(wasm_path).map_err(|err| anyhow::anyhow!("error reading wasm file: {}", err))
    }

    pub fn wasm_file_path(&self) -> Result<PathBuf> {
        if let Some(wasm_path) = self.wasm_file.as_ref() {
            Ok(wasm_path.to_path_buf())
        } else {
            let metadata = MetadataCommand::new()
                .manifest_path("./Cargo.toml")
                .features(CargoOpt::AllFeatures)
                .exec()?;

            let root_package = metadata
                .root_package()
                .ok_or_else(|| anyhow::anyhow!("unable to find root package".to_owned()))?;
            //print!("root package: {:#?}", metadata.);
            let project_name = &root_package.name;
            println!("project name: {:#?}", project_name);

            let asset_name = project_name.to_case(Case::Snake);

            let path = PathBuf::from(format!(
                "target/wasm32-unknown-unknown/{}/{}.wasm",
                self.release, asset_name
            ));
            Ok(path)
        }
    }
}
