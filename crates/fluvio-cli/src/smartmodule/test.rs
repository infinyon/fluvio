use std::collections::BTreeMap;

use fluvio_smartengine::SmartEngine;
use fluvio_spu_schema::server::stream_fetch::{LegacySmartModulePayload, SmartModuleWasmCompressed};
use tracing::debug;
use clap::Parser;

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


/// Create a new SmartModule with a given name
#[derive(Debug, Parser)]
pub struct TestSmartModuleOpt {
    /// The name of the SmartModule to generate
    name: String,

    // json value
    json: String,
}

impl TestSmartModuleOpt {
    pub async fn process(self, _fluvio: &Fluvio) -> Result<()> {
        
        // todo load from files
        let raw = vec![];

        let payload = LegacySmartModulePayload {
            wasm: SmartModuleWasmCompressed::Raw(raw),
            kind: SmartModuleKind::Map,
            params: BTreeMap::new().into(),
        };

        let engine = SmartEngine::default();
        let mut smartmodule = engine
            .create_module_from_payload(payload, None)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        // turn json string into raw
        let json_raw = self.json.as_bytes();
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
