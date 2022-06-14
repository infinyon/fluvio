use std::collections::BTreeMap;

use fluvio_smartengine::SmartEngine;
use fluvio_spu_schema::server::stream_fetch::{LegacySmartModulePayload, SmartModuleWasmCompressed};
use tracing::debug;
use clap::Parser;
use duct::{cmd};

use fluvio::{
    Fluvio,
    consumer::{SmartModuleKind},
    FluvioError,
    dataplane::{
        smartmodule::SmartModuleInput,
        record::{RecordData, Record},
    },
    RecordKey,
};

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

        let payload = LegacySmartModulePayload {
            wasm: SmartModuleWasmCompressed::Raw(raw),
            kind: SmartModuleKind::Map,
            params: BTreeMap::new().into(),
        };

        let engine = SmartEngine::default();
        let mut smartmodule = engine
            .create_module_from_payload(payload, None)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        let sample_input: Vec<u8> = vec![];

        let record_value: RecordData = sample_input.into();
        let entries = vec![Record::new_key_value(RecordKey::NULL, record_value)];
        let output = smartmodule
            .process(SmartModuleInput::try_from(entries)?)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;
        Ok(())
    }
}
