use quote::quote;
use proc_macro2::TokenStream;
use crate::{SmartStreamConfig, SmartStreamFn, SmartStreamKind};

pub fn generate_smartstream(config: &SmartStreamConfig, func: &SmartStreamFn) -> TokenStream {
    let decoding_section = generate_decoding();
    let encoding_section = generate_encoding();

    let user_code = &func.func;
    let stream_section = match config.kind {
        SmartStreamKind::Filter => generate_filter(func),
        SmartStreamKind::Map => generate_map(func),
    };

    quote! {
        #user_code

        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn filter(ptr: *mut u8, len: usize) -> i32 {
                #decoding_section

                #stream_section

                #encoding_section
            }
        }
    }
}

fn generate_decoding() -> TokenStream {
    quote! {
        extern "C" {
            fn copy_records(putr: i32, len: i32);
        }

        let input_data = Vec::from_raw_parts(ptr, len, len);
        let mut records: Vec<fluvio_smartstream::dataplane::record::Record> = vec![];
        if let Err(_err) = fluvio_smartstream::dataplane::core::Decoder::decode(&mut records, &mut std::io::Cursor::new(input_data), 0) {
            return -1;
        };
    }
}

fn generate_filter(func: &SmartStreamFn) -> TokenStream {
    let user_fn = &func.name;
    quote! {
        let mut processed: Vec<_> = records.into_iter()
            .filter(|record| super:: #user_fn(record))
            .collect();
    }
}

fn generate_map(func: &SmartStreamFn) -> TokenStream {
    let user_fn = &func.name;
    quote! {
        let mut processed: Vec<_> = records.into_iter()
            .map(|record| super:: #user_fn(record))
            .collect();
    }
}

fn generate_encoding() -> TokenStream {
    quote! {
        let mut out = vec![];
        if let Err(_) = fluvio_smartstream::dataplane::core::Encoder::encode(&mut processed, &mut out, 0) {
            return -1;
        }

        let out_len = out.len();
        let ptr = out.as_mut_ptr();
        std::mem::forget(out);

        copy_records(ptr as i32, out_len as i32);

        processed.len() as i32
    }
}
