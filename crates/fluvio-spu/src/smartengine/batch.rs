use std::time::Instant;
use std::io::Error as IoError;

use anyhow::Error;
use fluvio_compression::{Compression, CompressionError};
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use tracing::{instrument, debug, trace};

use fluvio_protocol::{Encoder};
use fluvio_protocol::{
    record::{Batch, MemoryRecords, Offset},
    link::smartmodule::SmartModuleTransformRuntimeError,
};
use fluvio_smartengine::SmartModuleChainInstance;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

pub(crate) trait SmartModuleInputBatch {
    fn records(&self) -> &Vec<u8>;

    fn base_offset(&self) -> Offset;

    fn offset_delta(&self) -> i32;

    fn get_compression(&self) -> Result<Compression, CompressionError>;
}

#[instrument(skip(sm_chain_instance, input_batches, max_bytes, metric))]
pub(crate) fn process_batch<R: SmartModuleInputBatch>(
    sm_chain_instance: &mut SmartModuleChainInstance,
    input_batches: &mut impl Iterator<Item = Result<R, IoError>>,
    max_bytes: usize,
    metric: &SmartModuleChainMetrics,
) -> Result<(Batch, Option<SmartModuleTransformRuntimeError>), Error> {
    let mut smartmodule_batch = Batch::<MemoryRecords>::default();
    smartmodule_batch.base_offset = -1; // indicate this is uninitialized
    smartmodule_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

    let mut total_bytes = 0;

    for batch_result in input_batches {
        let input_batch = batch_result?;

        debug!(
            current_batch_offset = input_batch.base_offset(),
            current_batch_offset_delta = input_batch.offset_delta(),
            smartmodule_offset_delta = smartmodule_batch.get_header().last_offset_delta,
            smartmodule_base_offset = smartmodule_batch.base_offset,
            smartmodule_records = smartmodule_batch.records().len(),
            "Starting SmartModuleInstance processing"
        );

        let now = Instant::now();

        let input = SmartModuleInput::new(input_batch.records().clone(), input_batch.base_offset());

        let output = sm_chain_instance.process(input, metric)?;

        debug!(smartmodule_execution_time = %now.elapsed().as_millis());

        let maybe_error = output.error;
        let mut records = output.successes;

        trace!("smartmodule processed records: {:#?}", records);

        // there are smartmoduleed records!!
        if records.is_empty() {
            debug!("smartmodules records empty");
        } else {
            if smartmodule_batch.base_offset == -1 {
                // set compression if this is the first time
                set_compression(&input_batch, &mut smartmodule_batch);

                // set base offset if this is first time
                smartmodule_batch.base_offset = input_batch.base_offset();
            }

            // difference between smartmodule batch and and current batch
            // since base are different we need update delta offset for each records
            let relative_base_offset = smartmodule_batch.base_offset - input_batch.base_offset();

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
                offset_delta = input_batch.offset_delta(),
                "adding to offset delta"
            );
            smartmodule_batch.add_to_offset_delta(input_batch.offset_delta() + 1);
        }

        // If we had a processing error, return current batch and error
        if maybe_error.is_some() {
            return Ok((smartmodule_batch, maybe_error));
        }
    }

    debug!(
        total_records = smartmodule_batch.records().len(),
        "No more batches, SmartModuleInstance end"
    );

    Ok((smartmodule_batch, None))
}

fn set_compression(
    input_batch: &impl SmartModuleInputBatch,
    smartmodule_batch: &mut Batch<MemoryRecords>,
) {
    match input_batch.get_compression() {
        Ok(compression) => smartmodule_batch.header.set_compression(compression),
        Err(e) => {
            debug!("Couldn't get compression value from batch: {}", e)
        }
    }
}
