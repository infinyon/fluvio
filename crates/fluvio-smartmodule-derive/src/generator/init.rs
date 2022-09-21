use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

// generate init smartmodule
pub fn generate_init_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_code = func.func;

    quote! {

        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod _system {

            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn init(ptr: *mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{
                    SmartModuleInitError, SmartModuleInitInput,
                    SmartModuleInitOutput,
                    SmartModuleInitErrorStatus
                };
                use fluvio_smartmodule::dataplane::core::{Decoder};

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut input = SmartModuleInitInput::default();
                if let Err(_err) =
                    Decoder::decode(&mut input, &mut std::io::Cursor::new(input_data), version)
                {
                    return SmartModuleInitErrorStatus::DecodingInput as i32;
                }

                let mut output = SmartModuleInitOutput {
                    error: None,
                };

                let result = super::init(input.params);

                /*

                let status = match result {
                    Ok(_) => 0,
                    Err(err) =>  {
                        let error = SmartModuleInitError::new(
                            err,
                        );
                        output.error = Some(error);
                        -1
                    }
                };

                let mut out = vec![];
                if let Err(_) = Encoder::encode(&output, &mut out, version) {
                    return SmartModuleInstanceProcessError::EncodingOutput as i32;
                }

                let out_len = out.len();
                let ptr = out.as_mut_ptr();
                std::mem::forget(out);
                copy_records(ptr as i32, out_len as i32);
                */
                0




            }
        }
    }
}
