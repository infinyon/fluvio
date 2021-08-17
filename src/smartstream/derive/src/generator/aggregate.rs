use quote::quote;
use proc_macro2::TokenStream;
use crate::SmartStreamFn;

pub fn generate_aggregate_smartstream(func: &SmartStreamFn) -> TokenStream {
    let user_code = &func.func;
    let user_fn = &func.name;

    quote! {
        #user_code

        mod __system {
            #[no_mangle]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn aggregate(ptr: &mut u8, len: usize) -> i32 {
                use fluvio_smartstream::dataplane::smartstream::{
                    SmartStreamAggregateInput, SmartStreamInternalError,
                    SmartStreamRuntimeError, SmartStreamType, SmartStreamOutput,
                };
                use fluvio_smartstream::dataplane::core::{Encoder, Decoder};
                use fluvio_smartstream::dataplane::record::{Record, RecordData};

                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let input_data = Vec::from_raw_parts(ptr, len, len);
                let mut smartstream_input = SmartStreamAggregateInput::default();
                if let Err(_err) = Decoder::decode(&mut smartstream_input, &mut std::io::Cursor::new(input_data), 0) {
                    return SmartStreamInternalError::DecodingBaseInput as i32;
                }

                let mut accumulator = smartstream_input.accumulator;
                let records_input = smartstream_input.base.record_data;
                let mut records: Vec<Record> = vec![];
                if let Err(_err) = Decoder::decode(&mut records, &mut std::io::Cursor::new(records_input), 0) {
                    return SmartStreamInternalError::DecodingRecords as i32;
                };

                // PROCESSING
                let mut output = SmartStreamOutput {
                    successes: Vec::with_capacity(records.len()),
                    error: None,
                };

                for mut record in records.into_iter() {
                    let acc_data = RecordData::from(accumulator);
                    let result = super:: #user_fn(acc_data, &record);

                    match result {
                        Ok(value) => {
                            accumulator = Vec::from(value.as_ref());
                            record.value = RecordData::from(accumulator.clone());
                            output.successes.push(record);
                        }
                        Err(err) => {
                            let error = SmartStreamRuntimeError::new(
                                &record,
                                smartstream_input.base.base_offset,
                                SmartStreamType::Aggregate,
                                err,
                            );
                            output.error = Some(error);
                            break;
                        }
                    }
                }

                // ENCODING
                let mut out = vec![];
                if let Err(_) = Encoder::encode(&mut output, &mut out, 0) {
                    return SmartStreamInternalError::EncodingOutput as i32;
                }

                let out_len = out.len();
                let ptr = out.as_mut_ptr();
                std::mem::forget(out);
                copy_records(ptr as i32, out_len as i32);
                output.successes.len() as i32
            }
        }
    }
}
