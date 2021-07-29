use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartStreamFn;

pub fn generate_map_smartstream(func: &SmartStreamFn) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    quote! {
        #user_code

        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn map(ptr: *mut u8, len: usize) -> i32 {

                // DECODING
                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut records: Vec<fluvio_smartstream::dataplane::record::Record> = vec![];
                if let Err(_err) = fluvio_smartstream::dataplane::core::Decoder::decode(&mut records, &mut std::io::Cursor::new(input_data), 0) {
                    return fluvio_smartstream::ENCODING_ERROR;
                };

                // PROCESSING
                let mut output = fluvio_smartstream::dataplane::smartstream::SmartStreamOutput {
                    successes: Vec::with_capacity(records.len()),
                    error: None,
                };

                for mut record in records.into_iter() {
                    let result = super:: #user_fn(&record);
                    match result {
                        Ok((maybe_key, value)) => {
                            record.key = maybe_key;
                            record.value = value;
                            output.successes.push(record);
                        }
                        Err(e) => {
                            let offset_delta = record.preamble.offset_delta();
                            let kind = fluvio_smartstream::dataplane::smartstream::SmartStreamType::Map;
                            let mut error = fluvio_smartstream::dataplane::smartstream::SmartStreamRuntimeErrorBuilder::new(e, offset_delta, kind);
                            error.record(&record);
                            output.error = Some(error);
                            break;
                        }
                    }
                }

                // ENCODING
                let mut out = vec![];
                if let Err(_) = fluvio_smartstream::dataplane::core::Encoder::encode(&mut output, &mut out, 0) {
                    return fluvio_smartstream::ENCODING_ERROR;
                }

                let out_len = out.len();
                let ptr = out.as_mut_ptr();
                std::mem::forget(out);

                copy_records(ptr as i32, out_len as i32);

                output.successes.len() as i32
            }
        }
    }
}
