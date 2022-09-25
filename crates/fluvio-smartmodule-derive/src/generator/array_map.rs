use quote::quote;
use proc_macro2::TokenStream;

use crate::SmartModuleFn;
use crate::util::ident;

use super::transform::generate_transform;

pub fn generate_array_map_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        ident("array_map"),
        user_code,
        quote! {

            for record in records.into_iter() {

                use fluvio_smartmodule::dataplane::record::RecordKey;

                let result = #function_call;
                match result {
                    Ok(output_records) => {
                        for (output_key, output_value) in output_records {
                            let key = RecordKey::from_option(output_key);
                            let new_record = Record::new_key_value(key, output_value);
                            output.successes.push(new_record);
                        }
                    }
                    Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record,
                            base_offset,
                            SmartModuleKind::ArrayMap,
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
