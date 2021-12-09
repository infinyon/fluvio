use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_lock::Mutex;
use async_channel::Sender;

use tracing::trace;

use dataplane::Offset;
use dataplane::record::Record;
use fluvio_types::PartitionId;
use fluvio_protocol::Encoder;

use crate::producer::record::{BatchMetadata, FutureRecordMetadata, PartialFutureRecordMetadata};
use crate::producer::ProducerError;
use crate::error::Result;

/// This struct acts as a queue that accumulates records into batches.
/// It is used by the producer to buffer records before sending them to the SPU.
/// The batches are separated by PartitionId
pub(crate) struct RecordAccumulator {
    batch_size: usize,
    batches: Arc<HashMap<PartitionId, Arc<Mutex<VecDeque<ProducerBatch>>>>>,
}

impl RecordAccumulator {
    pub(crate) fn new(batch_size: usize, partition_n: i32) -> Self {
        let mut batches = HashMap::default();
        for i in 0..partition_n {
            batches.insert(i, Arc::new(Mutex::new(VecDeque::new())));
        }
        Self {
            batches: Arc::new(batches),
            batch_size,
        }
    }

    /// Add a record to the accumulator.
    pub(crate) async fn push_record(
        &self,
        record: Record,
        partition_id: PartitionId,
    ) -> Result<PushRecord, ProducerError> {
        let batches_lock = self
            .batches
            .get(&partition_id)
            .ok_or(ProducerError::PartitionNotFound(partition_id))?;

        let mut batches = batches_lock.lock().await;
        if let Some(batch) = batches.back_mut() {
            if let Some(push_record) = batch.push_record(record.clone()) {
                return Ok(PushRecord::new(
                    push_record.into_future_record_metadata(partition_id),
                    batch.is_full(),
                    false,
                ));
            }
        }

        trace!(
            partition_id,
            "Batch is full. Creating a new batch for partition"
        );

        let mut batch = ProducerBatch::new(self.batch_size);

        match batch.push_record(record) {
            Some(push_record) => {
                let batch_is_full = batch.is_full();

                batches.push_back(batch);

                Ok(PushRecord::new(
                    push_record.into_future_record_metadata(partition_id),
                    batch_is_full,
                    true,
                ))
            }
            None => Err(ProducerError::RecordTooLarge(self.batch_size)),
        }
    }

    pub(crate) fn batches(&self) -> Arc<HashMap<PartitionId, Arc<Mutex<VecDeque<ProducerBatch>>>>> {
        self.batches.clone()
    }
}

pub(crate) struct PushRecord {
    pub(crate) future: FutureRecordMetadata,
    pub(crate) is_full: bool,
    pub(crate) new_batch: bool,
}

impl PushRecord {
    fn new(future: FutureRecordMetadata, is_full: bool, new_batch: bool) -> Self
where {
        Self {
            future,
            is_full,
            new_batch,
        }
    }
}

pub(crate) struct ProducerBatch {
    pub(crate) notify: Sender<Offset>,
    batch_metadata: Arc<BatchMetadata>,
    write_limit: usize,
    current_size: usize,
    is_full: bool,
    create_time: Instant,
    pub(crate) records: Vec<Record>,
}
impl ProducerBatch {
    fn new(write_limit: usize) -> Self {
        let now = Instant::now();
        let (sender, receiver) = async_channel::bounded(1);
        let batch_metadata = Arc::new(BatchMetadata::new(receiver));

        Self {
            notify: sender,
            batch_metadata,
            is_full: false,
            write_limit,
            create_time: now,
            current_size: 0,
            records: vec![],
        }
    }

    pub(crate) fn create_time(&self) -> &Instant {
        &self.create_time
    }

    /// Add a record to the batch.
    /// Return ProducerError::BatchFull if record does not fit in the batch, so
    /// the RecordAccumulator can create more batches if needed.
    fn push_record(&mut self, record: Record) -> Option<PartialFutureRecordMetadata> {
        let relative_offset = self.records.len() as i64;
        let record_size = record.write_size(0);

        if self.current_size + record_size > self.write_limit {
            self.is_full = true;
            return None;
        }

        self.current_size += record_size;

        self.records.push(record);

        Some(PartialFutureRecordMetadata::new(
            relative_offset,
            self.batch_metadata.clone(),
        ))
    }

    pub(crate) fn is_full(&self) -> bool {
        self.is_full || self.write_limit <= self.current_size
    }
}
