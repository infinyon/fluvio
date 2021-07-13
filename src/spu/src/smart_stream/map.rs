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
use dataplane::smartstream::SmartStreamOutput;

const MAP_FN_NAME: &str = "map";
type MapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamMap {
    store: Store<()>,
    instance: Instance,
    map_fn: MapFn,
    records_cb: Arc<RecordsCallBack>,
}

impl SmartStreamMap {
    pub fn new(engine: &SmartStreamEngine, module: &SmartStreamModule) -> Result<Self> {
        let mut store = Store::new(&engine.0, ());
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records = Func::wrap(
            &mut store,
            move |mut caller: Caller<'_, ()>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm map");
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
        let map_fn: MapFn = instance.get_typed_func(&mut store, MAP_FN_NAME)?;

        Ok(Self {
            store,
            instance,
            map_fn,
            records_cb,
        })
    }

    /// map batches with maximum bytes to be send back consumer
    pub fn map(&mut self, iter: &mut FileBatchIterator, max_bytes: usize) -> Result<Batch, Error> {
        let mut memory_filter_batch = Batch::<MemoryRecords>::default();
        memory_filter_batch.base_offset = -1; // indicate this is unitialized
        memory_filter_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            let file_batch = match iter.next() {
                // we filter-map entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is discarded
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = memory_filter_batch.records().len(),
                        "no more batches filter end"
                    );
                    return Ok(memory_filter_batch);
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                filter_offset_delta = memory_filter_batch.get_header().last_offset_delta,
                filter_base_offset = memory_filter_batch.base_offset,
                filter_records = memory_filter_batch.records().len(),
                "starting filter processing"
            );

            let now = Instant::now();

            self.records_cb.clear();

            let array_ptr = super::memory::copy_memory_to_instance(
                &mut self.store,
                &self.instance,
                &file_batch.records,
            )?;

            let filter_record_count = self.map_fn.call(
                &mut self.store,
                (array_ptr as i32, file_batch.records.len() as i32),
            )?;

            debug!(filter_record_count,filter_execution_time = %now.elapsed().as_millis());

            if filter_record_count == -1 {
                return Err(anyhow!("map failed"));
            }

            let output_bytes = self
                .records_cb
                .get()
                .and_then(|m| m.copy_memory_from(&mut self.store).ok())
                .unwrap_or_default();
            debug!(out_filter_bytes = output_bytes.len());

            // this is inefficient for now
            let mut output = SmartStreamOutput::default();
            output.decode(&mut Cursor::new(output_bytes), 0)?;
            let mut records = output.successes;

            // there are filtered records!!
            if records.is_empty() {
                debug!("filters records empty");
            } else {
                // set base offset if this is first time
                if memory_filter_batch.base_offset == -1 {
                    memory_filter_batch.base_offset = file_batch.base_offset();
                }

                // difference between filter batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset =
                    memory_filter_batch.base_offset - file_batch.base_offset();

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
                    return Ok(memory_filter_batch);
                }

                total_bytes += record_bytes;

                debug!(
                    filter_records = records.len(),
                    total_bytes, "finished filtering"
                );
                memory_filter_batch.mut_records().append(&mut records);
            }

            // only increment filter offset delta if filter_batch has been initialized
            if memory_filter_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                memory_filter_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }
        }
    }
}
