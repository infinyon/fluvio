use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_lock::Mutex;
use async_channel::Sender;

use tracing::{trace, instrument};

use dataplane::Offset;
use dataplane::record::Record;
use fluvio_types::PartitionId;
use fluvio_protocol::Encoder;

use crate::producer::record::{BatchMetadata, FutureRecordMetadata, PartialFutureRecordMetadata};
use crate::producer::ProducerError;
use crate::error::Result;

use super::event::EventHandler;

use instant::Instant;

pub(crate) type BatchHandler = (Arc<BatchEvents>, Arc<Mutex<VecDeque<ProducerBatch>>>);

const ENCODING_PROTOCOL_VERSION: i16 = 0;

/// This struct acts as a queue that accumulates records into batches.
/// It is used by the producer to buffer records before sending them to the SPU.
/// The batches are separated by PartitionId
pub(crate) struct RecordAccumulator {
    batch_size: usize,
    batches: Arc<HashMap<PartitionId, BatchHandler>>,
}

impl RecordAccumulator {
    pub(crate) fn new(batch_size: usize, partition_n: i32) -> Self {
        let mut batches = HashMap::default();
        for i in 0..partition_n {
            batches.insert(
                i,
                (BatchEvents::shared(), Arc::new(Mutex::new(VecDeque::new()))),
            );
        }
        Self {
            batches: Arc::new(batches),
            batch_size,
        }
    }

    /// Add a record to the accumulator.
    #[instrument(skip(self, record))]
    pub(crate) async fn push_record(
        &self,
        record: Record,
        partition_id: PartitionId,
    ) -> Result<PushRecord, ProducerError> {
        let (batch_events, batches_lock) = self
            .batches
            .get(&partition_id)
            .ok_or(ProducerError::PartitionNotFound(partition_id))?;

        let mut batches = batches_lock.lock().await;
        if let Some(batch) = batches.back_mut() {
            if let Some(push_record) = batch.push_record(record.clone()) {
                if batch.is_full() {
                    batch_events.notify_batch_full().await;
                }
                return Ok(PushRecord::new(
                    push_record.into_future_record_metadata(partition_id),
                ));
            } else {
                batch_events.notify_batch_full().await;
            }
        }

        trace!(
            partition_id,
            "Batch is full. Creating a new batch for partition"
        );

        let mut batch = ProducerBatch::new(self.batch_size);

        match batch.push_record(record) {
            Some(push_record) => {
                batch_events.notify_new_batch().await;

                if batch.is_full() {
                    batch_events.notify_batch_full().await;
                }

                batches.push_back(batch);

                Ok(PushRecord::new(
                    push_record.into_future_record_metadata(partition_id),
                ))
            }
            None => Err(ProducerError::RecordTooLarge(self.batch_size)),
        }
    }

    pub(crate) fn batches(&self) -> Arc<HashMap<PartitionId, BatchHandler>> {
        self.batches.clone()
    }
}

pub(crate) struct PushRecord {
    pub(crate) future: FutureRecordMetadata,
}

impl PushRecord {
    fn new(future: FutureRecordMetadata) -> Self
where {
        Self { future }
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
        let record_size = record.write_size(ENCODING_PROTOCOL_VERSION);

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

pub(crate) struct BatchEvents {
    batch_full: EventHandler,
    new_batch: EventHandler,
}

impl BatchEvents {
    fn new() -> Self {
        let batch_full = EventHandler::new();
        let new_batch = EventHandler::new();
        Self {
            batch_full,
            new_batch,
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub async fn listen_batch_full(&self) {
        self.batch_full.listen().await
    }

    pub async fn listen_new_batch(&self) {
        self.new_batch.listen().await
    }

    #[instrument(skip(self))]
    pub async fn notify_batch_full(&self) {
        trace!("Notifying batch full event");
        self.batch_full.notify().await;
    }

    #[instrument(skip(self))]
    pub async fn notify_new_batch(&self) {
        trace!("Notifying new batch event");
        self.new_batch.notify().await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dataplane::record::Record;

    #[test]
    fn test_producer_batch_push_and_not_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(ENCODING_PROTOCOL_VERSION);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(size * 3 + 1);

        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());

        assert!(!pb.is_full());

        assert!(pb.push_record(record).is_none());
    }

    #[test]
    fn test_producer_batch_push_and_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(ENCODING_PROTOCOL_VERSION);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(size * 3);

        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());

        assert!(pb.is_full());

        assert!(pb.push_record(record).is_none());
    }

    #[fluvio_future::test]
    async fn test_record_accumulator() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(ENCODING_PROTOCOL_VERSION);
        let accumulator = RecordAccumulator::new(size * 3, 1);
        let timeout = std::time::Duration::from_millis(200);

        let batches = accumulator
            .batches()
            .get(&0)
            .expect("failed to get batch info")
            .0
            .clone();

        accumulator
            .push_record(record.clone(), 0)
            .await
            .expect("failed push");
        assert!(
            async_std::future::timeout(timeout, batches.listen_new_batch())
                .await
                .is_ok()
        );

        assert!(
            async_std::future::timeout(timeout, batches.listen_batch_full())
                .await
                .is_err()
        );
        accumulator
            .push_record(record.clone(), 0)
            .await
            .expect("failed push");
        accumulator
            .push_record(record, 0)
            .await
            .expect("failed push");
        assert!(
            async_std::future::timeout(timeout, batches.listen_batch_full())
                .await
                .is_ok()
        );
        assert!(
            async_std::future::timeout(timeout, batches.listen_new_batch())
                .await
                .is_err()
        );
    }
}
