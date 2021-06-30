use std::sync::Arc;
use std::time::Instant;
use std::io::Cursor;

use anyhow::{Result, Error, anyhow};

use tracing::debug;
use wasmtime::{Caller, Extern, Func, Instance, Trap, TypedFunc, Store};

use dataplane::core::{Decoder, Encoder};
use dataplane::batch::Batch;
use dataplane::batch::MemoryRecords;
use crate::smart_stream::{RecordsCallBack, RecordsMemory, SmartStreamEngine, SmartStreamModule};
use crate::smart_stream::file_batch::FileBatchIterator;
use dataplane::smartstream::Aggregate;

const AGGREGATE_FN_NAME: &str = "aggregate";
type AggregateFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamAggregate {
    store: Store<()>,
    instance: Instance,
    aggregate_fn: AggregateFn,
    records_cb: Arc<RecordsCallBack>,
    accumulator: Vec<u8>,
}

impl SmartStreamAggregate {
    pub fn new(
        engine: &SmartStreamEngine,
        module: &SmartStreamModule,
        accumulator: Vec<u8>,
    ) -> Result<Self> {
        let mut store = Store::new(&engine.0, ());
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records = Func::wrap(
            &mut store,
            move |mut caller: Caller<'_, ()>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm filter");
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };

                let records = RecordsMemory { ptr, len, memory };
                cb.set(records);
                Ok(())
            },
        );

        let instance = Instance::new(&mut store, &module.0, &[copy_records.into()])?;
        let aggregate_fn: AggregateFn = instance.get_typed_func(&mut store, AGGREGATE_FN_NAME)?;

        Ok(Self {
            store,
            aggregate_fn,
            instance,
            records_cb,
            accumulator,
        })
    }

    /// filter batches with maximum bytes to be send back consumer
    pub fn aggregate(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<Batch, Error> {
        let mut aggregate_batch = Batch::<MemoryRecords>::default();
        aggregate_batch.base_offset = -1; // indicate this is uninitialized
        aggregate_batch.set_offset_delta(-1); // make add_to_offset_delta correctly
        let mut total_bytes = 0;

        loop {
            // Aggregate entire batches. Entire batches are process as group
            let file_batch = match iter.next() {
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = aggregate_batch.records().len(),
                        "no more batches filter end"
                    );
                    return Ok(aggregate_batch);
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                agg_offset_delta = aggregate_batch.get_header().last_offset_delta,
                agg_base_offset = aggregate_batch.base_offset,
                agg_records = aggregate_batch.records().len(),
                "starting aggregate processing"
            );

            let now = Instant::now();
            self.records_cb.clear();

            let aggregate_data = Aggregate {
                accumulator: Vec::from(accumulator),
                records: file_batch.records.clone(),
            };
            let mut aggregator_bytes = vec![];
            fluvio_protocol::Encoder::encode(&aggregate_data, &mut aggregator_bytes, 0)?;

            let aggregate_ptr = super::memory::copy_memory_to_instance(
                &mut self.store,
                &self.instance,
                &file_batch.records,
            )?;
            let aggregate_args = (aggregate_ptr as i32, aggregator_bytes.len() as i32);

            let aggregate_record_count = self.aggregate_fn.call(&mut self.store, aggregate_args)?;
            debug!(aggregate_record_count, filter_execution_time = %now.elapsed().as_millis());

            if aggregate_record_count == -1 {
                return Err(anyhow!("aggregate failed"));
            }

            let record_bytes = self
                .records_cb
                .get()
                .and_then(|m| m.copy_memory_from(&mut self.store).ok())
                .unwrap_or_default();
            debug!(out_filter_bytes = record_bytes.len());

            // this is inefficient for now
            let mut records: MemoryRecords = vec![];
            records.decode(&mut Cursor::new(record_bytes), 0)?;

            // there are filtered records!!
            if records.is_empty() {
                debug!("filters records empty");
            } else {
                // set base offset if this is first time
                if aggregate_batch.base_offset == -1 {
                    aggregate_batch.base_offset = file_batch.base_offset();
                }

                // difference between filter batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset = aggregate_batch.base_offset - file_batch.base_offset();

                for record in &mut records {
                    record.add_base_offset(relative_base_offset);
                }

                let record_bytes = records.write_size(0);

                // if filter bytes exceed max bytes then we skip this batch
                if total_bytes + record_bytes > max_bytes {
                    debug!(
                        total_bytes = total_bytes + record_bytes,
                        max_bytes, "total filter bytes reached"
                    );
                    return Ok(aggregate_batch);
                }

                total_bytes += record_bytes;

                debug!(
                    filter_records = records.len(),
                    total_bytes, "finished filtering"
                );
                aggregate_batch.mut_records().append(&mut records);
            }

            // only increment filter offset delta if filter_batch has been initialized
            if aggregate_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                aggregate_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }
        }
    }
}
