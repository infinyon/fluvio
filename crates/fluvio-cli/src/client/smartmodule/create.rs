use std::path::PathBuf;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::debug;
use async_trait::async_trait;
use clap::Parser;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec};
use fluvio_extension_common::Terminal;

use crate::Result;
use crate::client::cmd::ClientCmd;

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct CreateSmartModuleOpt {
    /// The name of the SmartModule to create
    name: String,
    /// The path to a WASM binary to create the SmartModule from
    #[clap(long)]
    wasm_file: PathBuf,
    #[clap(long)]
    /// The path to the SmartModule package (experimental)
    package: Option<PathBuf>,
}

#[async_trait]
impl ClientCmd for CreateSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<()> {
        /*
            * TODO: Fix the CRD to work with this
        let buffer = vec!['a' as u8; self.size];
        */

        /*
        // load package if provided
        let package_opt = if let Some(package_path) = self.package {
            let m = SmartModuleMetadata::from_file(package_path)?;
            println!("Using SmartModule package: {}", m.package.name);
            (
                Some(SmartModulePackage {
                    name: m.package.name.clone(),
                    version: m.package.version.clone(),
                    group: m.package.group.clone(),
                    ..Default::default()
                }),
                m.init
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            match v.input {
                                InitType::String => {
                                    SmartModuleInitParam::new(SmartModuleInitType::String)
                                }
                            },
                        )
                    })
                    .collect(),
            )
        } else {
            (None, BTreeMap::new())
        };
        */

        let raw = std::fs::read(self.wasm_file)?;

        let spec = SmartModuleSpec {
            wasm: SmartModuleWasm::from_raw_wasm_bytes(&raw)?,
            ..Default::default()
        };

        debug!(name = self.name, "creating smartmodule");
        let admin = fluvio.admin().await;
        admin.create(self.name.to_string(), false, spec).await?;
        println!("smartmodule \"{}\" has been created.", self.name);

        Ok(())
    }
}
