use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

// generate look_back
pub fn generate_look_back_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = func.name;
    let user_code = func.func;
    quote! {

        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod ___system {

            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn look_back(ptr: *mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{
                    SmartModuleInput, SmartModuleLookbackErrorStatus,
                    SmartModuleLookbackRuntimeError, SmartModuleKind, SmartModuleLookbackOutput,
                };
                use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
                use fluvio_smartmodule::dataplane::record::{Record, RecordData};

                // DECODING
                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut smartmodule_input = SmartModuleInput::default();
                if let Err(_err) = Decoder::decode(&mut smartmodule_input, &mut std::io::Cursor::new(input_data), version) {
                    return SmartModuleLookbackErrorStatus::DecodingBaseInput as i32;
                }

                let base_offset = smartmodule_input.base_offset();
                let records_input = smartmodule_input.into_raw_bytes();
                let mut records: Vec<Record> = vec![];
                if let Err(_err) = Decoder::decode(&mut records, &mut std::io::Cursor::new(records_input), version) {
                    return SmartModuleLookbackErrorStatus::DecodingRecords as i32;
                };

                // PROCESSING
                for record in records.into_iter() {
                    let result = super:: #user_fn(&record);
                    if let Err(err) = result {
                        let error = SmartModuleLookbackRuntimeError::new(
                            &record,
                            base_offset,
                            err,
                        );
                        let mut output = SmartModuleLookbackOutput {
                            error,
                        };

                        // ENCODING
                        let mut out = vec![];
                        if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
                            return SmartModuleLookbackErrorStatus::EncodingOutput as i32;
                        }

                        let out_len = out.len();
                        let out_ptr = out.as_mut_ptr();
                        std::mem::forget(out);
                        copy_records(out_ptr as i32, out_len as i32);

                        return SmartModuleLookbackErrorStatus::PropagatedError as i32;
                    };
                }
                0
            }
        }
    }
}
