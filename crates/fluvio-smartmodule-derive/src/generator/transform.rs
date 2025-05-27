use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleKind;
use crate::ast::SmartModuleFn;
use crate::generator::generate_records_code;
use crate::util::generate_ident;

pub(crate) fn generate_transform(
    sm_kind: SmartModuleKind,
    sm_func: &SmartModuleFn,
    transform: TokenStream,
) -> TokenStream {
    let user_code = &sm_func.func;
    let name = generate_ident(&sm_kind);
    let records_code = generate_records_code(sm_func, &sm_kind);

    quote! {
        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod __system {
            #[unsafe(no_mangle)]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn #name(ptr: *mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleTransformErrorStatus,
                    SmartModuleTransformRuntimeError, SmartModuleKind, SmartModuleOutput
                };

                // DECODING
                unsafe extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);

                #records_code

                let base_offset = smartmodule_input.base_offset();
                let base_timestamp = smartmodule_input.base_timestamp();

                // PROCESSING
                let mut output = SmartModuleOutput {
                    successes: Vec::with_capacity(records.len()),
                    error: None,
                };

                #transform

                // ENCODING
                let mut out = vec![];
                if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
                    return SmartModuleTransformErrorStatus::EncodingOutput as i32;
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
