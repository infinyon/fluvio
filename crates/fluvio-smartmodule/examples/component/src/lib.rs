use regex::Regex;
use once_cell::sync::OnceCell;

use fluvio_smartmodule::{smartmodule, Record, Result};


static REGEX: OnceCell<Regex> = OnceCell::new();

// r"^\d{4}-\d{2}-\d{2}$"
//#[smartmodule(config)]
/* 
pub fn init(config: SmartModuleConfig) {
    REGEX.set(Regex::new(config).unwrap());
}
*/

pub fn init() {
    REGEX.set(Regex::new("[0-9]{3}-[0-9]{3}-[0-9]{4}").unwrap()).unwrap();
}

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(REGEX.get().unwrap().is_match(string))
}
