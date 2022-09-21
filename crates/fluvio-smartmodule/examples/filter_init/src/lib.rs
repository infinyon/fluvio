use once_cell::sync::OnceCell;

use fluvio_smartmodule::{
    smartmodule, Record, Result,eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError},
};

static CRITERIA: OnceCell<String> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(key) = params.get("key") {
        CRITERIA.set(key.clone()).map_err(|err| eyre!("failed setting key: {:#?}", err))
    } else {
        Err(SmartModuleInitError::MissingParam("key".to_string()).into())
    }
}

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains(CRITERIA.get().unwrap()))
}
