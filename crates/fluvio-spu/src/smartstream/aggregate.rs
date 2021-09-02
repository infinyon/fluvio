use std::time::Instant;
use std::convert::TryFrom;

use anyhow::{Result, Error};

use tracing::debug;
use wasmtime::TypedFunc;

use dataplane::core::Encoder;
use dataplane::batch::Batch;
use dataplane::batch::MemoryRecords;
use crate::smartstream::{SmartStreamEngine, SmartStreamModule, SmartStreamContext};
use crate::smartstream::file_batch::FileBatchIterator;
use dataplane::smartstream::{
    SmartStreamRuntimeError, SmartStreamAggregateInput, SmartStreamInput, SmartStreamOutput,
    SmartStreamInternalError,
};

const AGGREGATE_FN_NAME: &str = "aggregate";
type AggregateFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamAggregate {
    base: SmartStreamContext,
    aggregate_fn: AggregateFn,
    accumulator: Vec<u8>,
}

impl SmartStreamAggregate {
    pub fn new(
        engine: &SmartStreamEngine,
        module: &SmartStreamModule,
        accumulator: Vec<u8>,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module)?;
        let aggregate_fn: AggregateFn = base
            .instance
            .get_typed_func(&mut base.store, AGGREGATE_FN_NAME)?;

        Ok(Self {
            base,
            aggregate_fn,
            accumulator,
        })
    }

    /// filter batches with maximum bytes to be send back consumer
    pub fn aggregate(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<(Batch, Option<SmartStreamRuntimeError>), Error> {
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
                    return Ok((aggregate_batch, None));
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
            self.base.records_cb.clear();

            let input = SmartStreamAggregateInput {
                base: SmartStreamInput {
                    base_offset: file_batch.batch.base_offset,
                    record_data: file_batch.records.clone(),
                },
                accumulator: self.accumulator.clone(),
            };
            let slice = self.base.write_input(&input)?;
            let aggregate_output = self.aggregate_fn.call(&mut self.base.store, slice)?;
            debug!(aggregate_output, filter_execution_time = %now.elapsed().as_millis());

            if aggregate_output < 0 {
                let internal_error = SmartStreamInternalError::try_from(aggregate_output)
                    .unwrap_or(SmartStreamInternalError::UnknownError);
                return Err(internal_error.into());
            }

            let output: SmartStreamOutput = self.base.read_output()?;
            let maybe_error = output.error;
            let mut records = output.successes;

            // there are filtered records!!
            if records.is_empty() {
                debug!("Aggregate records empty");
            } else {
                // If any records came back, take the last one and set it as
                // the new accumulator state.
                let latest_accumulator = records.iter().last();
                if let Some(latest) = latest_accumulator {
                    debug!(?latest, "Got most recent accumulator:");
                    self.accumulator = Vec::from(latest.value.as_ref());
                }

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
                    return Ok((aggregate_batch, maybe_error));
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

            // If we had a filtering error, return current batch and error
            if maybe_error.is_some() {
                return Ok((aggregate_batch, maybe_error));
            }
        }
    }
}
