use std::time::Instant;
use std::convert::TryFrom;

use anyhow::{Result, Error};

use tracing::debug;
use wasmtime::TypedFunc;

use dataplane::batch::Batch;
use dataplane::batch::MemoryRecords;
use dataplane::smartstream::{
    SmartStreamInput, SmartStreamOutput, SmartStreamRuntimeError, SmartStreamInternalError,
};
use fluvio_protocol::Encoder;
use crate::smartstream::{SmartStreamModule, SmartStreamEngine, SmartStreamBase};
use crate::smartstream::file_batch::FileBatchIterator;

const FILTER_FN_NAME: &str = "filter";
type FilterFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamFilter {
    base: SmartStreamBase,
    filter_fn: FilterFn,
}

impl SmartStreamFilter {
    pub fn new(engine: &SmartStreamEngine, module: &SmartStreamModule) -> Result<Self> {
        let mut base = SmartStreamBase::new(engine, module)?;
        let filter_fn: FilterFn = base
            .instance
            .get_typed_func(&mut base.store, FILTER_FN_NAME)?;

        Ok(Self { base, filter_fn })
    }

    /// filter batches with maximum bytes to be send back consumer
    pub fn filter(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<(Batch, Option<SmartStreamRuntimeError>), Error> {
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
                    return Ok((memory_filter_batch, None));
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

            let input = SmartStreamInput {
                base_offset: file_batch.batch.base_offset,
                record_data: file_batch.records.clone(),
            };
            let slice = self.base.write_input(&input)?;
            let filter_output = self.filter_fn.call(&mut self.base.store, slice)?;

            debug!(filter_output,filter_execution_time = %now.elapsed().as_millis());

            if filter_output < 0 {
                let internal_error = SmartStreamInternalError::try_from(filter_output)
                    .unwrap_or(SmartStreamInternalError::UnknownError);
                return Err(internal_error.into());
            }

            let output: SmartStreamOutput = self.base.read_output()?;
            let maybe_error = output.error;
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
                    return Ok((memory_filter_batch, maybe_error));
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

            // If we had a filtering error, return current batch and error
            if maybe_error.is_some() {
                return Ok((memory_filter_batch, maybe_error));
            }
        }
    }
}
