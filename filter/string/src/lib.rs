use fluvio_smartstream_wasm as smart_stream;

use crate::smart_stream::dataplane::record::DefaultRecord;
use crate::smart_stream::dataplane::core::{Decoder, Encoder};

extern "C" {
    fn copy_records(putr: i32, len: i32);
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn filter(ptr: *mut u8, len: usize) -> i32 {
    use std::io::Cursor;

    let input_data = Vec::from_raw_parts(ptr, len, len);
    let mut records: Vec<DefaultRecord> = vec![];

    if let Err(_err) = records.decode(&mut Cursor::new(input_data), 0) {
        return -1;
    };

    let filter_records: Vec<DefaultRecord> = records
        .into_iter()
        .filter(|r| {
            let value = String::from_utf8_lossy(r.value().as_ref());
            value.contains('a')
        })
        .collect();

    /*
    let filter_records: Vec<DefaultRecord> = records
    .into_iter()
    .filter_map(|r| {
        let value = String::from_utf8_lossy(r.value().as_ref());
        let mut new_record = DefaultRecord::default();
        new_record.value = value.to_ascii_uppercase().as_bytes().into();
        Some(new_record)
    })
    .collect();
    */

    // encode back
    let mut out = vec![];
    if let Err(_err) = filter_records.encode(&mut out, 0) {
        return -1;
    }

    let out_len = out.len();

    let ptr = out.as_mut_ptr();
    std::mem::forget(out);

    copy_records(ptr as i32, out_len as i32);

    filter_records.len() as i32
}
