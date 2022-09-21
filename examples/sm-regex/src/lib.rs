use regex::Regex;
use once_cell::sync::OnceCell;

use fluvio_smartmodule::{
    smartmodule, Record, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError},
};

static REGEX: OnceCell<Regex> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(regex) = params.get("regex") {
        REGEX
            .set(Regex::new(regex)?)
            .map_err(|err| eyre!("regex init: {:#?}", err))
    } else {
        Err(SmartModuleInitError::MissingParam("regex".to_string()).into())
    }
}

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(REGEX.get().unwrap().is_match(string))
}
