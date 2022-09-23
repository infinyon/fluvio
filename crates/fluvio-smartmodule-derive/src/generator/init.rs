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
                    SmartModuleInitErrorStatus,
                    SmartModuleInitRuntimeError
                };
                use fluvio_smartmodule::dataplane::core::{Decoder,Encoder};




                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut input = SmartModuleInitInput::default();
                if let Err(_err) =
                    Decoder::decode(&mut input, &mut std::io::Cursor::new(input_data), version)
                {
                    return SmartModuleInitErrorStatus::DecodingInput as i32;
                }


                match super::init(input.params) {
                    Ok(_) => 0,
                    Err(err) =>  {

                        // copy data from wasm memory
                        extern "C" {
                            fn copy_records(putr: i32, len: i32);
                        }

                        let mut output = SmartModuleInitOutput {
                            error: SmartModuleInitRuntimeError::new(err)
                        };


                        let mut out = vec![];
                        if let Err(_) = Encoder::encode(&output, &mut out, version) {
                            return SmartModuleInitErrorStatus::EncodingOutput as i32;
                        }


                        let out_len = out.len();
                        let ptr = out.as_mut_ptr();
                        std::mem::forget(out);
                        copy_records(ptr as i32, out_len as i32);

                        SmartModuleInitErrorStatus::InitError as i32
                    }
                }

            }
        }
    }
}
