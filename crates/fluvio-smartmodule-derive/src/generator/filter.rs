use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_filter_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = func.func;

    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        ident("filter"),
        user_code,
        quote! {
            for mut record in records.into_iter() {

                let result = #function_call;
                match result {
                    Ok(value) => {
                        if value {
                            output.successes.push(record);
                        }
                    }
                    Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record,
                            base_offset,
                            SmartModuleKind::Filter,
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
