use std::path::PathBuf;
use clap::Parser;
use anyhow::Result;

use fluvio::FluvioConfig;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec, SmartModuleMetadata};
use fluvio_extension_common::target::ClusterTarget;
use fluvio::Fluvio;
use fluvio_future::task::run_block_on;
use crate::package::{PackageInfo, PackageOption};

pub const DEFAULT_META_LOCATION: &str = "SmartModule.toml";

/// Load SmartModule into Fluvio cluster
#[derive(Debug, Parser)]
pub struct LoadOpt {
    #[clap(long)]
    name: Option<String>,

    /// Optional path to SmartModule package directory
    #[clap(long)]
    package_path: Option<PathBuf>,

    #[clap(flatten)]
    package: PackageOption,

    /// Optional wasm file path
    #[clap(long)]
    wasm_file: Option<PathBuf>,

    #[clap(flatten)]
    target: ClusterTarget,

    /// Validate package config files, and connection to cluster.
    /// Skip SmartModule load to cluster
    #[clap(long, action)]
    dry_run: bool,
}
impl LoadOpt {
    pub(crate) fn process(&self) -> Result<()> {
        if let Some(path) = &self.package_path {
            std::env::set_current_dir(path)?;
        }

        println!("Loading package at: {}", std::env::current_dir()?.display());

        // resolve the current cargo project
        let package_info =
            PackageInfo::from_options(&self.package).map_err(|e| anyhow::anyhow!(e))?;

        // load ./SmartModule.toml relative to the project root
        let mut sm_toml = package_info.package_path.clone();
        sm_toml.push(DEFAULT_META_LOCATION);
        let pkg_metadata = SmartModuleMetadata::from_toml(sm_toml.clone())?;
        println!("Found SmartModule package: {}", pkg_metadata.package.name);

        // Check for empty group
        if pkg_metadata.package.group.is_empty() {
            eprintln!("Please set a value for `group` in {}", sm_toml.display());
            std::process::exit(1);
        }

        let sm_id = pkg_metadata.package.name.clone(); // pass anything, this should be overriden by SC
        let raw_bytes = match &self.wasm_file {
            Some(wasm_file) => crate::read_bytes_from_path(wasm_file)?,
            None => package_info.read_bytes()?,
        };

        let spec = SmartModuleSpec {
            meta: Some(pkg_metadata),
            wasm: SmartModuleWasm::from_raw_wasm_bytes(&raw_bytes)?,
            ..Default::default()
        };

        let fluvio_config = self.target.clone().load()?;

        if let Err(e) = run_block_on(self.create(fluvio_config, spec, sm_id)) {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        Ok(())
    }

    async fn create(&self, config: FluvioConfig, spec: SmartModuleSpec, id: String) -> Result<()> {
        println!("Trying connection to fluvio {}", config.endpoint);
        let fluvio = Fluvio::connect_with_config(&config).await?;

        let admin = fluvio.admin().await;

        if !self.dry_run {
            println!("Creating SmartModule: {}", id);
            admin.create(id, false, spec).await?;
        } else {
            println!("Dry run mode: Skipping SmartModule create");
        }

        Ok(())
    }
}
