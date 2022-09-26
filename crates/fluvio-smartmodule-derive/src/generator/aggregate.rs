use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

pub fn generate_aggregate_smartmodule(func: &SmartModuleFn) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    let function_call = quote!(
        super:: #user_fn(acc_data, &record)
    );

    quote! {

        #[allow(dead_code)]
        #user_code

        #[cfg(target_arch = "wasm32")]
        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn aggregate(ptr: &mut u8, len: usize, version: i16) -> i32 {
                use fluvio_smartmodule::dataplane::smartmodule::{
                    SmartModuleAggregateInput, SmartModuleTransformErrorStatus,
                    SmartModuleTransformRuntimeError, SmartModuleKind, SmartModuleOutput,SmartModuleAggregateOutput
                };
                use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
                use fluvio_smartmodule::dataplane::record::{Record, RecordData};

                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut smartmodule_input = SmartModuleAggregateInput::default();
                // 13 is version for aggregate
                if let Err(_err) = Decoder::decode(&mut smartmodule_input, &mut std::io::Cursor::new(input_data), version) {
                    return SmartModuleTransformErrorStatus::DecodingBaseInput as i32;
                }

                let mut accumulator = smartmodule_input.accumulator;

                let base_offset = smartmodule_input.base.base_offset();
                let records_input = smartmodule_input.base.into_raw_bytes();
                let mut records: Vec<Record> = vec![];
                if let Err(_err) = Decoder::decode(&mut records, &mut std::io::Cursor::new(records_input), version) {
                    return SmartModuleTransformErrorStatus::DecodingRecords as i32;
                };

                // PROCESSING
                let mut output = SmartModuleAggregateOutput {
                    base: SmartModuleOutput {
                        successes: Vec::with_capacity(records.len()),
                        error: None,
                    },
                    accumulator: accumulator.clone(),
                };

                for mut record in records.into_iter() {
                    let acc_data = RecordData::from(accumulator);
                    let result = #function_call;

                    match result {
                        Ok(value) => {
                            accumulator = Vec::from(value.as_ref());
                            output.accumulator = accumulator.clone();
                            record.value = RecordData::from(output.accumulator.clone());
                            output.base.successes.push(record);
                        }
                        Err(err) => {
                            let error = SmartModuleTransformRuntimeError::new(
                                &record,
                                base_offset,
                                SmartModuleKind::Aggregate,
                                err,
                            );
                            output.base.error = Some(error);
                            break;
                        }
                    }
                }

                let output_len = output.base.successes.len() as i32;

                // ENCODING
                let mut out = vec![];
                if let Err(_) = Encoder::encode(&mut output, &mut out, version) {
                    return SmartModuleTransformErrorStatus::EncodingOutput as i32;
                }

                let out_len = out.len();
                let ptr = out.as_mut_ptr();
                std::mem::forget(out);
                copy_records(ptr as i32, out_len as i32);
                output_len
            }
        }
    }
}
