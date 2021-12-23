use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_lock::Mutex;
use async_channel::Sender;

use event_listener::{Event, EventListener};
use tracing::trace;

use dataplane::Offset;
use dataplane::record::Record;
use fluvio_types::PartitionId;
use fluvio_protocol::Encoder;

use crate::producer::record::{BatchMetadata, FutureRecordMetadata, PartialFutureRecordMetadata};
use crate::producer::ProducerError;
use crate::error::Result;

pub(crate) struct BatchEvents {
    batch_full: Event,
    new_batch: Event,
}

impl BatchEvents {
    fn new() -> Self {
        let batch_full = Event::new();
        let new_batch = Event::new();
        Self {
            batch_full,
            new_batch,
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn listen_batch_full(&self) -> EventListener {
        self.batch_full.listen()
    }

    pub fn listen_new_batch(&self) -> EventListener {
        self.new_batch.listen()
    }

    pub fn notify_batch_full(&self) {
        self.batch_full.notify(usize::MAX);
    }

    pub fn notify_new_batch(&self) {
        self.new_batch.notify(usize::MAX);
    }
}

pub(crate) type BatchHandler = (Arc<BatchEvents>, Arc<Mutex<VecDeque<ProducerBatch>>>);
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
                    batch_events.notify_batch_full();
                }
                return Ok(PushRecord::new(
                    push_record.into_future_record_metadata(partition_id),
                ));
            } else {
                batch_events.notify_batch_full();
            }
        }

        trace!(
            partition_id,
            "Batch is full. Creating a new batch for partition"
        );

        let mut batch = ProducerBatch::new(self.batch_size);

        match batch.push_record(record) {
            Some(push_record) => {
                batch_events.notify_new_batch();

                if batch.is_full() {
                    batch_events.notify_batch_full();
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
