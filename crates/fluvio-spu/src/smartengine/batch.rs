use std::time::Instant;

use anyhow::Error;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use tracing::{instrument, debug, trace};

use fluvio_protocol::{Encoder};
use fluvio_protocol::{
    record::{Batch, MemoryRecords},
    link::smartmodule::SmartModuleTransformRuntimeError,
};
use fluvio_smartengine::SmartModuleChainInstance;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

use super::file_batch::FileBatchIterator;

pub(crate) trait BatchSmartEngine {
    fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        metric: &SmartModuleChainMetrics,
    ) -> Result<(Batch, Option<SmartModuleTransformRuntimeError>), Error>;
}

impl BatchSmartEngine for SmartModuleChainInstance {
    #[instrument(skip(self, iter, max_bytes, metric))]
    fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        metric: &SmartModuleChainMetrics,
    ) -> Result<(Batch, Option<SmartModuleTransformRuntimeError>), Error> {
        let mut smartmodule_batch = Batch::<MemoryRecords>::default();
        smartmodule_batch.base_offset = -1; // indicate this is uninitialized
        smartmodule_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            let file_batch = match iter.next() {
                // we process entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is discarded
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = smartmodule_batch.records().len(),
                        "No more batches, SmartModuleInstance end"
                    );
                    return Ok((smartmodule_batch, None));
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                smartmodule_offset_delta = smartmodule_batch.get_header().last_offset_delta,
                smartmodule_base_offset = smartmodule_batch.base_offset,
                smartmodule_records = smartmodule_batch.records().len(),
                "Starting SmartModuleInstance processing"
            );

            let now = Instant::now();

            //  let mut join_record = vec![];
            //  join_last_record.encode(&mut join_record, 0)?;

            let input =
                SmartModuleInput::new(file_batch.records.clone(), file_batch.batch.base_offset);

            let output = self.process(input, metric)?;
            debug!(smartmodule_execution_time = %now.elapsed().as_millis());

            let maybe_error = output.error;
            let mut records = output.successes;

            trace!("smartmodule processed records: {:#?}", records);

            // there are smartmoduleed records!!
            if records.is_empty() {
                debug!("smartmodules records empty");
            } else {
                // set base offset if this is first time
                if smartmodule_batch.base_offset == -1 {
                    smartmodule_batch.base_offset = file_batch.base_offset();
                }

                // difference between smartmodule batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset = smartmodule_batch.base_offset - file_batch.base_offset();

                for record in &mut records {
                    record.add_base_offset(relative_base_offset);
                }

                let record_bytes = records.write_size(0);

                // if smartmodule bytes exceed max bytes then we skip this batch
                if total_bytes + record_bytes > max_bytes {
                    debug!(
                        total_bytes = total_bytes + record_bytes,
                        max_bytes, "Total SmartModuleInstance bytes reached"
                    );
                    return Ok((smartmodule_batch, maybe_error));
                }

                total_bytes += record_bytes;

                debug!(
                    smartmodule_records = records.len(),
                    total_bytes, "finished smartmoduleing"
                );
                smartmodule_batch.mut_records().append(&mut records);
            }

            // only increment smartmodule offset delta if smartmodule_batch has been initialized
            if smartmodule_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                smartmodule_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }

            // If we had a processing error, return current batch and error
            if maybe_error.is_some() {
                return Ok((smartmodule_batch, maybe_error));
            }
        }
    }
}
