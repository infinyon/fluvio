use std::{collections::BTreeMap, path::PathBuf};

use clap::Parser;

use fluvio_smartengine::SmartEngine;
use fluvio_spu_schema::server::stream_fetch::{LegacySmartModulePayload, SmartModuleWasmCompressed};

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

use crate::{Result, error::CliError};

/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct TestSmartModuleOpt {
    // json value
    #[clap(long)]
    json: Option<String>,

    // arbitrary file
    #[clap(long)]
    file: Option<PathBuf>,

    #[clap(long)]
    wasm_file: PathBuf,
}

impl TestSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        // load wasm file
        let raw = std::fs::read(self.wasm_file)?;

        let payload = LegacySmartModulePayload {
            wasm: SmartModuleWasmCompressed::Raw(raw),
            kind: SmartModuleKind::ArrayMap,
            params: BTreeMap::new().into(),
        };

        let engine = SmartEngine::default();
        let mut smartmodule = engine
            .create_module_from_payload(payload, None)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        // get raw json in one of other ways
        let json_raw = if let Some(json) = self.json {
            json.as_bytes().to_vec()
        } else {
            if let Some(json_file) = &self.file {
                std::fs::read(json_file)?
            } else {
                return Err(CliError::Other("No json provided".to_string()));
            }
        };

        let record_value: RecordData = json_raw.into();
        let entries = vec![Record::new_key_value(RecordKey::NULL, record_value)];
        let output = smartmodule
            .process(SmartModuleInput::try_from(entries)?)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        let output_record = output.successes.first().unwrap();
        let output_value = output_record
            .value
            .as_str()
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;
        println!("{}", output_value);

        Ok(())
    }
}
