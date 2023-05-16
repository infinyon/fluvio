mod mapping;
mod model;
mod pointer;
mod transform;

use once_cell::sync::OnceCell;

use crate::mapping::Mapping;
use eyre::ContextCompat;
use fluvio_smartmodule::{
    dataplane::smartmodule::SmartModuleExtraParams, smartmodule, Record, RecordData, Result,
};

static MAPPING: OnceCell<Mapping> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(raw_mapping) = params.get("mapping") {
        match serde_json::from_str(raw_mapping) {
            Ok(mapping) => {
                MAPPING
                    .set(mapping)
                    .expect("mapping is already initialized");
                Ok(())
            }
            Err(err) => {
                eprintln!("unable to parse init params: {err:?}");
                Err(eyre::Report::msg("could not parse json-sql mapping"))
            }
        }
    } else {
        Err(eyre::Report::msg("no json-sql mapping supplied"))
    }
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let mapping = MAPPING
        .get()
        .wrap_err("json-sql mapping is not initialized")?;

    let key = record.key.clone();
    let record = serde_json::from_slice(record.value.as_ref())?;
    let transformed = transform::transform(record, mapping)?;

    Ok((key, serde_json::to_vec(&transformed)?.into()))
}
