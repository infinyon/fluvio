use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_map_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        ident("map"),
        user_code,
        quote! {
            for mut record in records.into_iter() {
                let result = #function_call;
                match result {
                    Ok((maybe_key, value)) => {
                        record.key = maybe_key;
                        record.value = value;
                        output.successes.push(record);
                    }
                    Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record,
                            base_offset,
                            SmartModuleKind::Map,
                            err,
                        );
                        output.error = Some(error);
                        break;
                    }
                }
            }

        },
    )
}
