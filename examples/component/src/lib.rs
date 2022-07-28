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


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn init(ptr: *mut u8, len: usize, version: i16) -> i32 {
    use fluvio_smartmodule::dataplane::smartmodule::{
        SmartModuleInput, SmartModuleInternalError,
        SmartModuleRuntimeError, SmartModuleKind, SmartModuleOutput,
        SmartModuleExtraParams
    };
    use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
    use fluvio_smartmodule::dataplane::record::{Record, RecordData};

    // DECODING
    extern "C" {
        fn copy_records(putr: i32, len: i32);
    }

    let input_data = Vec::from_raw_parts(ptr, len, len);
    let mut params = SmartModuleExtraParams::default();
    if let Err(_err) = Decoder::decode(&mut params, &mut std::io::Cursor::new(input_data), version) {
        return SmartModuleInternalError::DecodingBaseInput as i32;
    }



    // ENCODING
    let mut out = vec![];
    if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
        return SmartModuleInternalError::EncodingOutput as i32;
    }

    let out_len = out.len();
    let ptr = out.as_mut_ptr();
    std::mem::forget(out);
    copy_records(ptr as i32, out_len as i32);
    output.successes.len() as i32
}



#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(REGEX.get().unwrap().is_match(string))
}
