use quote::quote;
use proc_macro2::TokenStream;

use crate::{SmartModuleFn, SmartModuleKind};

use super::transform::generate_transform;

pub fn generate_array_map_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_fn = &func.name;
    let function_call = quote!(
        super:: #user_fn(&record)
    );

    generate_transform(
        SmartModuleKind::ArrayMap,
        func,
        quote! {
            for mut record in records.into_iter() {
                let result = #function_call;

                match result {
                    Ok(output_records) => {
                        use fluvio_smartmodule::dataplane::record::RecordKey;

                        for (output_key, output_value) in output_records {
                            let key = RecordKey::from_option(output_key);
                            let new_record = Record::new_key_value(key, output_value);
                            output.successes.push(new_record.into());
                        }
                    }
                    Err(err) => {
                        let error = SmartModuleTransformRuntimeError::new(
                            &record.into(),
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
