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

//#[smartmodule(init)]
//pub fn init() {
//    REGEX.set(Regex::new("[0-9]{3}-[0-9]{3}-[0-9]{4}").unwrap()).unwrap();
//}

/*
#[smartmodule(init)]
pub fn init(params: SmartModuleExtraParams) {
    if let Some(regex) = params.get("regex") {
        REGEX.set(Regex::new(regex).unwrap()).unwrap();
        0
    } else {
        -1
    }
}
*/

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn init(ptr: *mut u8, len: usize, version: i16) -> i32 {
    use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInternalError, SmartModuleExtraParams};
    use fluvio_smartmodule::dataplane::core::{Decoder};

    let input_data = Vec::from_raw_parts(ptr, len, len);
    let mut params = SmartModuleExtraParams::default();
    if let Err(_err) = Decoder::decode(&mut params, &mut std::io::Cursor::new(input_data), version)
    {
        return SmartModuleInternalError::ParsingExtraParams as i32;
    }

    // get regex
    if let Some(regex) = params.get("regex") {
        REGEX.set(Regex::new(regex).unwrap()).unwrap();
        0
    } else {
        -1
    }
}

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(REGEX.get().unwrap().is_match(string))
}
