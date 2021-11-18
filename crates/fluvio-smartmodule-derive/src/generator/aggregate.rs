use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartModuleFn;

pub fn generate_aggregate_smartmodule(func: &SmartModuleFn, has_params: bool) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    let params_parsing = if has_params {
        quote!(
            use std::convert::TryInto;

            let params = match smartstream_input.base.params.try_into(){
                Ok(params) => params,
                Err(err) => return SmartStreamInternalError::ParsingExtraParams as i32,
            };

        )
    } else {
        quote!()
    };

    let function_call = if has_params {
        quote!(
            super:: #user_fn(acc_data, &record, &params)
        )
    } else {
        quote!(
            super:: #user_fn(acc_data, &record)
        )
    };

    quote! {
        #user_code

        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn aggregate(ptr: &mut u8, len: usize) -> i32 {
                use fluvio_smartmodule::dataplane::smartstream::{
                    SmartStreamAggregateInput, SmartStreamInternalError,
                    SmartModuleRuntimeError, SmartModuleKind, SmartStreamOutput,SmartStreamAggregateOutput
                };
                use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};
                use fluvio_smartmodule::dataplane::record::{Record, RecordData};

                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut smartstream_input = SmartStreamAggregateInput::default();
                // 13 is version for aggregate
                if let Err(_err) = Decoder::decode(&mut smartstream_input, &mut std::io::Cursor::new(input_data), fluvio_smartmodule::api_versions::AGGREGATOR_API) {
                    return SmartStreamInternalError::DecodingBaseInput as i32;
                }

                let mut accumulator = smartstream_input.accumulator;

                #params_parsing

                let records_input = smartstream_input.base.record_data;
                let mut records: Vec<Record> = vec![];
                if let Err(_err) = Decoder::decode(&mut records, &mut std::io::Cursor::new(records_input), fluvio_smartmodule::api_versions::AGGREGATOR_API) {
                    return SmartStreamInternalError::DecodingRecords as i32;
                };

                // PROCESSING
                let mut output = SmartStreamAggregateOutput {
                    base: SmartStreamOutput {
                        successes: Vec::with_capacity(records.len()),
                        error: None,
                    },
                    accumulator: Vec::new()
                };

                for mut record in records.into_iter() {
                    let acc_data = RecordData::from(accumulator);
                    let result = #function_call;

                    match result {
                        Ok(value) => {
                            accumulator = Vec::from(value.as_ref());
                            output.accumulator = accumulator.clone();
                            record.value = RecordData::from(accumulator.clone());
                            output.base.successes.push(record);
                        }
                        Err(err) => {
                            let error = SmartModuleRuntimeError::new(
                                &record,
                                smartstream_input.base.base_offset,
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
                if let Err(_) = Encoder::encode(&mut output, &mut out, fluvio_smartmodule::api_versions::AGGREGATOR_API) {
                    return SmartStreamInternalError::EncodingOutput as i32;
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
