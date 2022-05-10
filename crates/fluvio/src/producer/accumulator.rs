use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_lock::Mutex;
use async_channel::Sender;

use dataplane::batch::{Batch, memory::MemoryBatch};
use fluvio_compression::Compression;
use tracing::trace;

use dataplane::{Offset, ErrorCode};
use dataplane::record::Record;
use fluvio_types::{PartitionId, Timestamp};

use crate::producer::record::{BatchMetadata, FutureRecordMetadata, PartialFutureRecordMetadata};
use crate::producer::ProducerError;
use crate::error::Result;

use super::event::EventHandler;

pub(crate) type BatchHandler = (Arc<BatchEvents>, Arc<Mutex<VecDeque<ProducerBatch>>>);

/// This struct acts as a queue that accumulates records into batches.
/// It is used by the producer to buffer records before sending them to the SPU.
/// The batches are separated by PartitionId
pub(crate) struct RecordAccumulator {
    batch_size: usize,
    batches: Arc<HashMap<PartitionId, BatchHandler>>,
    compression: Compression,
}

impl RecordAccumulator {
    pub(crate) fn new(batch_size: usize, partition_n: i32, compression: Compression) -> Self {
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
            compression,
        }
    }

    /// Add a record to the accumulator.
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

        let mut batch = ProducerBatch::new(self.batch_size, self.compression);

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
    pub(crate) notify: Sender<(Offset, ErrorCode)>,
    batch_metadata: Arc<BatchMetadata>,
    batch: MemoryBatch,
}

impl ProducerBatch {
    fn new(write_limit: usize, compression: Compression) -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        let batch_metadata = Arc::new(BatchMetadata::new(receiver));
        let batch = MemoryBatch::new(write_limit, compression);

        Self {
            notify: sender,
            batch_metadata,
            batch,
        }
    }

    /// Add a record to the batch.
    /// Return ProducerError::BatchFull if record does not fit in the batch, so
    /// the RecordAccumulator can create more batches if needed.
    fn push_record(&mut self, record: Record) -> Option<PartialFutureRecordMetadata> {
        match self.batch.push_record(record) {
            None => None,
            Some(relative_offset) => Some(PartialFutureRecordMetadata::new(
                relative_offset,
                self.batch_metadata.clone(),
            )),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.batch.is_full()
    }

    pub(crate) fn elapsed(&self) -> Timestamp {
        self.batch.elapsed()
    }

    pub(crate) fn batch(self) -> Batch {
        self.batch.into()
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

    pub async fn notify_batch_full(&self) {
        self.batch_full.notify().await;
    }

    pub async fn notify_new_batch(&self) {
        self.new_batch.notify().await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dataplane::{record::Record, batch::RawRecords};
    use fluvio_protocol::Encoder;

    #[test]
    fn test_producer_batch_push_and_not_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(
            size * 3
                + 1
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());

        assert!(!pb.is_full());

        assert!(pb.push_record(record).is_none());
    }

    #[test]
    fn test_producer_batch_push_and_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());
        assert!(pb.push_record(record.clone()).is_some());

        assert!(pb.is_full());

        assert!(pb.push_record(record).is_none());
    }

    #[fluvio_future::test]
    async fn test_record_accumulator() {
        let record = Record::from(("key", "value"));

        let size = record.write_size(0);
        let accumulator = RecordAccumulator::new(
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            1,
            Compression::None,
        );
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

        assert!(
            async_std::future::timeout(timeout, batches.listen_batch_full())
                .await
                .is_err()
        );
        accumulator
            .push_record(record, 0)
            .await
            .expect("failed push");

        assert!(
            async_std::future::timeout(timeout, batches.listen_batch_full())
                .await
                .is_ok()
        );
    }
}
