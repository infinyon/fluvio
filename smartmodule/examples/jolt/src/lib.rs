use eyre::ContextCompat;
use once_cell::sync::OnceCell;

use fluvio_jolt::TransformSpec;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError};
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

static SPEC: OnceCell<TransformSpec> = OnceCell::new();

const PARAM_NAME: &str = "spec";

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(raw_spec) = params.get(PARAM_NAME) {
        match serde_json::from_str(raw_spec) {
            Ok(spec) => {
                SPEC.set(spec).expect("spec is already initialized");
                Ok(())
            }
            Err(err) => {
                eprintln!("unable to parse spec from params: {err:?}");
                Err(eyre::Report::msg(
                    "could not parse the specification from `spec` param",
                ))
            }
        }
    } else {
        Err(SmartModuleInitError::MissingParam(PARAM_NAME.to_string()).into())
    }
}

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let spec = SPEC.get().wrap_err("jolt spec is not initialized")?;

    let key = record.key.clone();
    let record = serde_json::from_slice(record.value.as_ref())?;
    let transformed = fluvio_jolt::transform(record, spec)?;
    let transformed = serde_json::to_string(&transformed)?;

    Ok((key, transformed.into()))
}
