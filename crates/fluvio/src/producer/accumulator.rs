use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_channel::Sender;
use async_lock::RwLock;
use tracing::trace;
use futures_util::future::{BoxFuture, Either, Shared};
use futures_util::{FutureExt, ready};

use fluvio_future::sync::{Mutex, MutexGuard};
use fluvio_future::sync::Condvar;
use fluvio_protocol::record::Batch;
use fluvio_compression::Compression;
use fluvio_protocol::record::Offset;
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::produce::ProduceResponse;
use fluvio_protocol::record::Record;
use fluvio_socket::SocketError;
use fluvio_types::{PartitionId, Timestamp, PartitionCount};

use crate::producer::record::{BatchMetadata, FutureRecordMetadata, PartialFutureRecordMetadata};
use crate::producer::ProducerError;
use crate::error::Result;

use super::event::EventHandler;
use super::memory_batch::{MemoryBatch, MemoryBatchStatus};

const RECORD_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) type BatchHandler = (Arc<BatchEvents>, Arc<BatchesDeque>);

pub(crate) struct BatchesDeque {
    pub batches: Mutex<VecDeque<ProducerBatch>>,
    pub control: Condvar,
}

impl BatchesDeque {
    pub(crate) fn new() -> Self {
        Self {
            batches: Mutex::new(VecDeque::new()),
            control: Condvar::new(),
        }
    }

    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }
}
/// This struct acts as a queue that accumulates records into batches.
/// It is used by the producer to buffer records before sending them to the SPU.
/// The batches are separated by PartitionId
pub(crate) struct RecordAccumulator {
    batch_size: usize,
    max_request_size: usize,
    queue_size: usize,
    batches: Arc<RwLock<HashMap<PartitionId, BatchHandler>>>,
    compression: Compression,
}

impl RecordAccumulator {
    pub(crate) fn new(
        batch_size: usize,
        max_request_size: usize,
        queue_size: usize,
        partition_n: PartitionCount,
        compression: Compression,
    ) -> Self {
        let batches = (0..partition_n)
            .map(|p| (p, (BatchEvents::shared(), BatchesDeque::shared())))
            .collect::<HashMap<_, _>>();
        Self {
            batches: Arc::new(RwLock::new(batches)),
            max_request_size,
            batch_size,
            compression,
            queue_size,
        }
    }

    pub(crate) async fn add_partition(
        &self,
        partition_id: PartitionId,
        value: (Arc<BatchEvents>, Arc<BatchesDeque>),
    ) -> BatchHandler {
        self.batches
            .write()
            .await
            .insert(partition_id, value.clone());

        value
    }

    /// Add a record to the accumulator.
    pub(crate) async fn push_record(
        &self,
        record: Record,
        partition_id: PartitionId,
    ) -> Result<PushRecord, ProducerError> {
        let batches_lock = self.batches.read().await;
        let (batch_events, batches_lock) = batches_lock
            .get(&partition_id)
            .ok_or(ProducerError::PartitionNotFound(partition_id))?;

        // Wait for space in the batch queue
        let mut batches = self.wait_for_space(batches_lock).await?;

        // If the last batch is not full, push the record to it
        if let Some(batch) = batches.back_mut() {
            match batch.push_record(record) {
                Ok(ProduceBatchStatus::Added(push_record)) => {
                    if batch.is_full() {
                        batch_events.notify_batch_full().await;
                    }
                    return Ok(PushRecord::new(
                        push_record.into_future_record_metadata(partition_id),
                    ));
                }
                Ok(ProduceBatchStatus::NotAdded(record)) => {
                    if batch.is_full() {
                        batch_events.notify_batch_full().await;
                    }

                    // Create and push a new batch if needed
                    let push_record = self
                        .create_and_new_batch(batch_events, &mut batches, record, 1)
                        .await?;

                    return Ok(PushRecord::new(
                        push_record.into_future_record_metadata(partition_id),
                    ));
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        trace!(partition_id, "Creating a new batch");

        // Create and push a new batch if needed
        let push_record = self
            .create_and_new_batch(batch_events, &mut batches, record, 1)
            .await?;

        Ok(PushRecord::new(
            push_record.into_future_record_metadata(partition_id),
        ))
    }

    async fn wait_for_space<'a>(
        &self,
        batches_lock: &'a Arc<BatchesDeque>,
    ) -> Result<MutexGuard<'a, VecDeque<ProducerBatch>>, ProducerError> {
        let mut batches = batches_lock.batches.lock().await;
        if batches.len() >= self.queue_size {
            let (guard, wait_result) = batches_lock
                .control
                .wait_timeout_until(batches, RECORD_ENQUEUE_TIMEOUT, |queue| {
                    queue.len() < self.queue_size
                })
                .await;
            if wait_result.timed_out() {
                return Err(ProducerError::BatchQueueWaitTimeout);
            }
            batches = guard;
        }
        Ok(batches)
    }

    async fn create_and_new_batch(
        &self,
        batch_events: &BatchEvents,
        batches: &mut VecDeque<ProducerBatch>,
        record: Record,
        attempts: usize,
    ) -> Result<PartialFutureRecordMetadata, ProducerError> {
        if attempts > 2 {
            // This should never happen, but if it does, we should stop the recursion
            return Err(ProducerError::Internal(
                "Attempts exceeded while creating a new batch".to_string(),
            ));
        }

        let mut batch =
            ProducerBatch::new(self.max_request_size, self.batch_size, self.compression);

        match batch.push_record(record) {
            Ok(ProduceBatchStatus::Added(push_record)) => {
                batch_events.notify_new_batch().await;
                if batch.is_full() {
                    batch_events.notify_batch_full().await;
                }

                batches.push_back(batch);
                Ok(push_record)
            }
            Ok(ProduceBatchStatus::NotAdded(record)) => {
                batch_events.notify_new_batch().await;
                if batch.is_full() {
                    batch_events.notify_batch_full().await;
                }

                batches.push_back(batch);
                // Box the future to avoid infinite size due to recursion
                Box::pin(self.create_and_new_batch(batch_events, batches, record, attempts + 1))
                    .await
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn batches(&self) -> HashMap<PartitionId, BatchHandler> {
        self.batches.read().await.clone()
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

enum ProduceBatchStatus {
    Added(PartialFutureRecordMetadata),
    NotAdded(Record),
}

pub(crate) struct ProducerBatch {
    pub(crate) notify: Sender<ProducePartitionResponseFuture>,
    batch_metadata: Arc<BatchMetadata>,
    batch: MemoryBatch,
}
impl ProducerBatch {
    fn new(write_limit: usize, batch_limit: usize, compression: Compression) -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        let batch_metadata = Arc::new(BatchMetadata::new(receiver));
        let batch = MemoryBatch::new(write_limit, batch_limit, compression);

        Self {
            notify: sender,
            batch_metadata,
            batch,
        }
    }

    /// Add a record to the batch.
    /// Return ProducerError::BatchFull if record does not fit in the batch, so
    /// the RecordAccumulator can create more batches if needed.
    fn push_record(&mut self, record: Record) -> Result<ProduceBatchStatus, ProducerError> {
        match self.batch.push_record(record) {
            Ok(MemoryBatchStatus::Added(offset)) => Ok(ProduceBatchStatus::Added(
                PartialFutureRecordMetadata::new(offset, self.batch_metadata.clone()),
            )),
            Ok(MemoryBatchStatus::NotAdded(record)) => Ok(ProduceBatchStatus::NotAdded(record)),
            Err(err) => Err(err),
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

type ProduceResponseFuture = Shared<BoxFuture<'static, Arc<Result<ProduceResponse, SocketError>>>>;

/// A Future that resolves to pair `base_offset` and `error_code`, which effectively come from
/// [`PartitionProduceResponse`].
pub(crate) struct ProducePartitionResponseFuture {
    inner: Either<(ProduceResponseFuture, usize), Option<(Offset, ErrorCode)>>,
}

impl ProducePartitionResponseFuture {
    /// Returns immediately available future from given offset and error.
    pub(crate) fn ready(offset: Offset, error: ErrorCode) -> Self {
        Self {
            inner: Either::Right(Some((offset, error))),
        }
    }

    /// Returns a future that firstly will resolve [`ProduceResponse`] from the given `response_fut`,
    /// and then will look up the partition response using `num`. [`ProduceResponseFuture`] is usually
    /// shared between other [`ProducePartitionResponseFuture`] and will be resolved only once and
    ///the response will be re-used.
    pub(crate) fn from(response_fut: ProduceResponseFuture, num: usize) -> Self {
        Self {
            inner: Either::Left((response_fut, num)),
        }
    }
}

impl Future for ProducePartitionResponseFuture {
    type Output = (Offset, ErrorCode);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().inner {
            Either::Left(ref mut pair) => {
                let response = ready!((pair.0).poll_unpin(cx));
                match response.as_ref() {
                    Ok(response) => Poll::Ready(
                        response
                            .responses
                            .iter()
                            .flat_map(|t| &t.partitions)
                            .nth(pair.1)
                            .map(|p| (p.base_offset, ErrorCode::None))
                            .unwrap_or_else(|| {
                                (
                                    Offset::default(),
                                    ErrorCode::Other(
                                        "partition not found during collecting async response"
                                            .to_string(),
                                    ),
                                )
                            }),
                    ),
                    Err(err) => Poll::Ready((0, ErrorCode::Other(format!("{err:?}")))),
                }
            }
            Either::Right(ref mut maybe_pair) => match maybe_pair.take() {
                None => Poll::Ready((0, ErrorCode::Other("empty response".to_string()))),
                Some(pair) => Poll::Ready(pair),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use fluvio_protocol::record::{Record, RawRecords};
    use fluvio_spu_schema::produce::{PartitionProduceResponse, TopicProduceResponse};
    use fluvio_protocol::Encoder;

    #[test]
    fn test_producer_batch_push_and_not_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(
            1_048_576,
            size * 3
                + 1
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));

        assert!(!pb.is_full());

        assert!(matches!(
            pb.push_record(record),
            Ok(ProduceBatchStatus::NotAdded(_))
        ));
    }

    #[test]
    fn test_producer_batch_push_and_full() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(
            1_048_576,
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));

        assert!(pb.is_full());

        assert!(matches!(
            pb.push_record(record),
            Ok(ProduceBatchStatus::NotAdded(_))
        ));
    }

    #[test]
    fn test_producer_write_limit() {
        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        // Producer batch that can store three instances of Record::from(("key", "value"))
        let mut pb = ProducerBatch::new(
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));
        assert!(matches!(
            pb.push_record(record.clone()),
            Ok(ProduceBatchStatus::Added(_))
        ));

        assert!(pb.is_full());

        assert!(pb.push_record(record).is_err());
    }

    #[fluvio_future::test]
    async fn test_record_accumulator() {
        let record = Record::from(("key", "value"));

        let size = record.write_size(0);
        let accumulator = RecordAccumulator::new(
            size * 3
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            1_048_576,
            10,
            1,
            Compression::None,
        );
        let timeout = std::time::Duration::from_millis(200);

        let batches = accumulator
            .batches()
            .await
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

        let record_2 = Record::from(("key_2", "value_2"));
        let batch_events = BatchEvents::shared();
        let batches_deque = BatchesDeque::shared();
        accumulator
            .add_partition(1, (batch_events.clone(), batches_deque.clone()))
            .await;
        accumulator
            .push_record(record_2.clone(), 1)
            .await
            .expect("failed push");

        let batches = accumulator
            .batches()
            .await
            .get(&1)
            .expect("failed to get batch info")
            .0
            .clone();

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
    }

    #[fluvio_future::test]
    async fn test_produce_partition_response_future_ready() {
        //given
        let offset = 10;
        let error_code = ErrorCode::default();
        let fut = ProducePartitionResponseFuture::ready(offset, error_code.clone());

        //when
        let (resolved_offset, resolved_error) = fut.await;

        //then
        assert_eq!(offset, resolved_offset);
        assert_eq!(error_code, resolved_error);
    }

    #[fluvio_future::test]
    async fn test_produce_partition_response_future_on_error() {
        //given
        let num = 0;
        let fut = async { Arc::new(Err(SocketError::SocketClosed)) }
            .boxed()
            .shared();
        let fut = ProducePartitionResponseFuture::from(fut, num);

        //when
        let (resolved_offset, resolved_error) = fut.await;

        //then
        assert_eq!(resolved_offset, 0);
        assert_eq!(resolved_error, ErrorCode::Other("SocketClosed".to_string()));
    }

    #[fluvio_future::test]
    async fn test_produce_partition_response_future_resolved() {
        //given
        let num = 2;
        let fut = async {
            Arc::new(Ok(ProduceResponse {
                responses: vec![
                    TopicProduceResponse {
                        name: "".to_string(),
                        partitions: vec![
                            PartitionProduceResponse {
                                base_offset: 1,
                                ..Default::default()
                            },
                            PartitionProduceResponse {
                                base_offset: 2,
                                ..Default::default()
                            },
                        ],
                    },
                    TopicProduceResponse {
                        name: "".to_string(),
                        partitions: vec![PartitionProduceResponse {
                            base_offset: 3,
                            ..Default::default()
                        }],
                    },
                ],
                throttle_time_ms: 0,
            }))
        }
        .boxed()
        .shared();
        let fut = ProducePartitionResponseFuture::from(fut, num);

        //when
        let (resolved_offset, resolved_error) = fut.await;

        //then
        assert_eq!(resolved_offset, 3);
        assert_eq!(resolved_error, ErrorCode::None);
    }

    #[fluvio_future::test]
    async fn test_produce_partition_response_future_not_found() {
        //given
        let num = 2;
        let fut = async {
            Arc::new(Ok(ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: "".to_string(),
                    partitions: vec![PartitionProduceResponse {
                        base_offset: 3,
                        ..Default::default()
                    }],
                }],
                throttle_time_ms: 0,
            }))
        }
        .boxed()
        .shared();
        let fut = ProducePartitionResponseFuture::from(fut, num);

        //when
        let (resolved_offset, resolved_error) = fut.await;

        //then
        assert_eq!(resolved_offset, 0);
        assert_eq!(
            resolved_error,
            ErrorCode::Other("partition not found during collecting async response".to_string())
        );
    }
}
