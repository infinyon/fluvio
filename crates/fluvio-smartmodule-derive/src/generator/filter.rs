use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_filter_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;

    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        ident("filter"),
        func,
        quote! {
            for mut record in records.into_iter() {
                use fluvio_smartmodule::SmartModuleRecord;

                let result = #function_call;

                match result {
                    Ok(value) => {
                        if value {
                            output.successes.push(record.into());
                        }
                    }
                    Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record.into(),
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
