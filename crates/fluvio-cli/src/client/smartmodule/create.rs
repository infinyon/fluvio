use std::collections::BTreeMap;
use std::path::PathBuf;
use std::io::Read;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use fluvio_extension_common::Terminal;
use fluvio_sc_schema::smartmodule::{SmartModulePackage, SmartModuleInitType, SmartModuleInitParam};
use tracing::debug;
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::smartmodule::{SmartModuleWasm, SmartModuleSpec};
use flate2::{Compression, bufread::GzEncoder};
use fluvio_smartmodule_package::package::{SmartModuleMetadata, InitType};

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
    /// The path to the source code for the SmartModule WASM
    #[clap(long)]
    _source_file: Option<PathBuf>,
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
        let raw = std::fs::read(self.wasm_file)?;
        let mut encoder = GzEncoder::new(raw.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw.len());
        encoder.read_to_end(&mut buffer)?;
        /*
            * TODO: Fix the CRD to work with this
        let buffer = vec!['a' as u8; self.size];
        */

        // load package if provided
        let package_opt = if let Some(package_path) = self.package {
            let m = SmartModuleMetadata::from_file(package_path)?;
            println!("Using SmartModule package: {}", m.package.name);
            (
                Some(SmartModulePackage {
                    name: m.package.name.clone(),
                    version: m.package.version.clone(),
                    group: m.package.group.clone(),
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

        let spec: SmartModuleSpec = SmartModuleSpec {
            wasm: SmartModuleWasm::from_binary_payload(buffer),
            package: package_opt.0,
            init_params: package_opt.1,
            ..Default::default()
        };

        debug!(name = self.name, "creating smart-module");
        let admin = fluvio.admin().await;
        admin.create(self.name.to_string(), false, spec).await?;
        println!("smart-module \"{}\" has been created.", self.name);

        Ok(())
    }
}
