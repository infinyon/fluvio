use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

// generate init smartmodule
pub fn generate_init_smartmodule(func: &SmartModuleFn) -> TokenStream {
    // let user_fn = &func.name;
    let user_code = func.func;

    quote! {
        #user_code

        mod __system {
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



            }
        }
    }
}
