use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartStreamFn;

pub fn generate_filter_smartstream(func: &SmartStreamFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = func.func;

    quote! {
        #user_code

        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn filter(ptr: *mut u8, len: usize) -> i32 {
                // DECODING
                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut records: Vec<fluvio_smartstream::dataplane::record::Record> = vec![];
                if let Err(_err) = fluvio_smartstream::dataplane::core::Decoder::decode(&mut records, &mut std::io::Cursor::new(input_data), 0) {
                    return -1;
                };

                // PROCESSING
                let mut output = fluvio_smartstream::dataplane::smartstream::SmartStreamOutput {
                    successes: Vec::with_capacity(records.len()),
                    error: None,
                };

                for mut record in records.into_iter() {
                    let result = super:: #user_fn(&record);
                    match result {
                        Ok(value) => {
                            if value {
                                output.successes.push(record);
                            }
                        }
                        Err(e) => {
                            output.error = Some(fluvio_smartstream::dataplane::smartstream::SmartStreamError::from(e));
                            break;
                        }
                    }
                }

                // ENCODING
                let mut out = vec![];
                if let Err(_) = fluvio_smartstream::dataplane::core::Encoder::encode(&mut output, &mut out, 0) {
                    return -1;
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
