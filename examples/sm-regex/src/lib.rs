use regex::Regex;
use once_cell::sync::OnceCell;

use fluvio_smartmodule::{
    smartmodule, Record, Result,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInternalError},
};

static REGEX: OnceCell<Regex> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> i32 {
    if let Some(regex) = params.get("regex") {
        REGEX.set(Regex::new(regex).unwrap()).unwrap();
        0
    } else {
        SmartModuleInternalError::InitParamsNotFound as i32
    }
}

/* 
#[smartmodule(init)]
fn init(regex: &str) -> i32 {
    REGEX.set(Regex::new(regex).unwrap()).unwrap();
    0
    
}
*/

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(REGEX.get().unwrap().is_match(string))
}
