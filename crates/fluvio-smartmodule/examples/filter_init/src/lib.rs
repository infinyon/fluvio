use once_cell::sync::OnceCell;

use fluvio_smartmodule::{smartmodule, Record, Result, dataplane::smartmodule::{SmartModuleExtraParams,SmartModuleInternalError}};


static CRITERIA: OnceCell<String> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(regex) = params.get("key") {
        CRITERIA.set(regex.clone()).unwrap();
        0
    } else {
        SmartModuleInternalError::InitParamsNotFound as i32
    }
}


#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains(CRITERIA.get().unwrap()))
}
