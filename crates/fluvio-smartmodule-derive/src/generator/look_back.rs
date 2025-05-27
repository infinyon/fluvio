use proc_macro2::TokenStream;
use quote::quote;

use crate::generator::{SmartModuleFn, SmartModuleKind, generate_records_code};

// generate look_back
pub fn generate_look_back_smartmodule(sm_func: &SmartModuleFn) -> TokenStream {
    let user_fn = sm_func.name;
    let user_code = sm_func.func;
    let records_code = generate_records_code(sm_func, &SmartModuleKind::LookBack);

    quote! {
        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod ___system {

            #[unsafe(no_mangle)]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn look_back(ptr: *mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{
                    SmartModuleLookbackErrorStatus,
                    SmartModuleLookbackRuntimeError, SmartModuleKind, SmartModuleLookbackOutput
                };

                // DECODING
                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);

                #records_code

                let base_offset = smartmodule_input.base_offset();

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
