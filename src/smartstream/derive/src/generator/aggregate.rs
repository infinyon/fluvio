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
            pub unsafe fn aggregate(agg_ptr: &mut u8, agg_len: usize) -> i32 {
                extern "C" {
                    fn copy_records(putr: i32, len: i32);
                }

                let agg_input = Vec::from_raw_parts(agg_ptr, agg_len, agg_len);
                let mut aggregate: fluvio_smartstream::dataplane::smartstream::Aggregate = Default::default();
                if let Err(_) = fluvio_smartstream::dataplane::core::Decoder::decode(&mut aggregate, &mut std::io::Cursor::new(agg_input), 0) {
                    return -1;
                }

                let mut accumulator = aggregate.accumulator;
                let record_bytes = aggregate.records;
                let mut records: Vec<fluvio_smartstream::dataplane::record::Record> = vec![];
                if let Err(_err) = fluvio_smartstream::dataplane::core::Decoder::decode(&mut records, &mut std::io::Cursor::new(record_bytes), 0) {
                    return -1;
                };

                let mut processed: Vec<fluvio_smartstream::dataplane::record::Record> = Vec::with_capacity(records.len());
                for mut record in records.into_iter() {
                    let acc_data = fluvio_smartstream::dataplane::record::RecordData::from(accumulator);
                    let output = super:: #user_fn (acc_data, &record);
                    accumulator = Vec::from(output.as_ref());
                    record.value = fluvio_smartstream::dataplane::record::RecordData::from(accumulator.clone());
                    processed.push(record);
                }

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
    }
}
