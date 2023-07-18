use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_filter_map_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;

    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        ident("filter_map"),
        func,
        quote! {
                for mut record in records.into_iter() {
                    use fluvio_smartmodule::SmartModuleRecord;

                    let result = #function_call;

                    match result {
                        Ok(Some((maybe_key, value))) => {
                            record.key = maybe_key;
                            record.value = value;
                            output.successes.push(record.into());
                        }
                        Ok(None) => {},
                        Err(err) => {
                            let error = SmartModuleTransformRuntimeError::new(
                                &record.into(),
                                base_offset,
                                SmartModuleKind::FilterMap,
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
