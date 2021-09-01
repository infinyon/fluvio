use std::time::Instant;
use std::convert::TryFrom;

use anyhow::{Result, Error};

use tracing::debug;
use wasmtime::TypedFunc;

use dataplane::core::Encoder;
use dataplane::batch::Batch;
use dataplane::batch::MemoryRecords;
use dataplane::smartstream::{
    SmartStreamInput, SmartStreamOutput, SmartStreamRuntimeError, SmartStreamInternalError,
};
use crate::smartstream::{SmartStreamEngine, SmartStreamModule, SmartStreamBase};
use crate::smartstream::file_batch::FileBatchIterator;

const MAP_FN_NAME: &str = "map";
type MapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamMap {
    base: SmartStreamBase,
    map_fn: MapFn,
}

impl SmartStreamMap {
    pub fn new(engine: &SmartStreamEngine, module: &SmartStreamModule) -> Result<Self> {
        let mut base = SmartStreamBase::new(engine, module)?;
        let map_fn: MapFn = base.instance.get_typed_func(&mut base.store, MAP_FN_NAME)?;

        Ok(Self { base, map_fn })
    }

    /// map batches with maximum bytes to be send back consumer
    pub fn map(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<(Batch, Option<SmartStreamRuntimeError>), Error> {
        let mut memory_map_batch = Batch::<MemoryRecords>::default();
        memory_map_batch.base_offset = -1; // indicate this is unitialized
        memory_map_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            let file_batch = match iter.next() {
                // we map entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is discarded
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = memory_map_batch.records().len(),
                        "no more batches map end"
                    );
                    return Ok((memory_map_batch, None));
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                map_offset_delta = memory_map_batch.get_header().last_offset_delta,
                map_base_offset = memory_map_batch.base_offset,
                map_records = memory_map_batch.records().len(),
                "starting map processing"
            );

            let now = Instant::now();

            let input = SmartStreamInput {
                base_offset: file_batch.batch.base_offset,
                record_data: file_batch.records.clone(),
            };
            let slice = self.base.write_input(&input)?;
            let map_output = self.map_fn.call(&mut self.base.store, slice)?;

            debug!(map_output, map_execution_time = %now.elapsed().as_millis());

            if map_output < 0 {
                let internal_error = SmartStreamInternalError::try_from(map_output)
                    .unwrap_or(SmartStreamInternalError::UnknownError);
                return Err(internal_error.into());
            }

            let output: SmartStreamOutput = self.base.read_output()?;
            let maybe_error = output.error;
            let mut records = output.successes;

            // there are mapped records!!
            if records.is_empty() {
                debug!("map records empty");
            } else {
                // set base offset if this is first time
                if memory_map_batch.base_offset == -1 {
                    memory_map_batch.base_offset = file_batch.base_offset();
                }

                // difference between map batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset = memory_map_batch.base_offset - file_batch.base_offset();

                for record in &mut records {
                    record.add_base_offset(relative_base_offset);
                }

                let record_bytes = records.write_size(0);

                // if map bytes exceed max bytes then we skip this batch
                if total_bytes + record_bytes > max_bytes {
                    debug!(
                        total_bytes = total_bytes + record_bytes,
                        max_bytes, "total map bytes reached"
                    );
                    return Ok((memory_map_batch, maybe_error));
                }

                total_bytes += record_bytes;

                debug!(map_records = records.len(), total_bytes, "finished mapping");
                memory_map_batch.mut_records().append(&mut records);
            }

            // only increment map offset delta if map_batch has been initialized
            if memory_map_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                memory_map_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }

            // If we had a mapping error, return current batch and error
            if maybe_error.is_some() {
                return Ok((memory_map_batch, maybe_error));
            }
        }
    }
}
