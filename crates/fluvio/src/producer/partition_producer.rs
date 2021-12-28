use std::sync::Arc;
use std::collections::VecDeque;
use std::time::Duration;

use async_lock::{Mutex, RwLock};
use dataplane::ReplicaKey;
use dataplane::batch::Batch;
use dataplane::produce::{DefaultPartitionRequest, DefaultTopicRequest, DefaultProduceRequest};
use fluvio_future::timer::sleep;
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use tracing::{debug, info, instrument, error, trace};

use crate::error::{Result, FluvioError};
use crate::spu::SpuPool;

use super::ProducerError;
use super::accumulator::{ProducerBatch, BatchEvents};
use super::event::EventHandler;

/// Struct that is responsible for sending produce requests to the SPU in a given partition.
pub(crate) struct PartitionProducer {
    replica: ReplicaKey,
    spu_pool: Arc<SpuPool>,
    batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
    batch_events: Arc<BatchEvents>,
    linger: Duration,
    last_error: Arc<RwLock<Option<ProducerError>>>,
}

impl PartitionProducer {
    fn new(
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        linger: Duration,
        last_error: Arc<RwLock<Option<ProducerError>>>,
    ) -> Self {
        Self {
            replica,
            spu_pool,
            batches_lock,
            batch_events,
            linger,
            last_error,
        }
    }

    pub fn shared(
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        linger: Duration,
        error: Arc<RwLock<Option<ProducerError>>>,
    ) -> Arc<Self> {
        Arc::new(PartitionProducer::new(
            replica,
            spu_pool,
            batches,
            batch_events,
            linger,
            error,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        linger: Duration,
        error: Arc<RwLock<Option<ProducerError>>>,
        end_event: Arc<StickyEvent>,
        flush_event: (Arc<EventHandler>, Arc<EventHandler>),
    ) {
        let producer =
            PartitionProducer::shared(replica, spu_pool, batches, batch_events, linger, error);
        fluvio_future::task::spawn(async move {
            producer.run(end_event, flush_event).await;
        });
    }

    #[instrument(skip(self, end_event, flush_event))]
    async fn run(
        &self,
        end_event: Arc<StickyEvent>,
        flush_event: (Arc<EventHandler>, Arc<EventHandler>),
    ) {
        use tokio::select;

        let mut linger_sleep = sleep(std::time::Duration::from_secs(1800));

        loop {
            select! {
                _ = end_event.listen() => {
                    info!("partition producer end event received");
                    break;
                },
                _ = flush_event.0.listen() => {

                    debug!("flush event received");
                    if let Err(e) = self.flush(true).await {
                        error!("Failed to flush producer: {}", e);
                        self.set_error(e).await;
                        flush_event.1.notify().await;

                        continue;
                    }
                    flush_event.1.notify().await;
                    linger_sleep = sleep(std::time::Duration::from_secs(1800));

                }
                _ =  self.batch_events.listen_batch_full() => {
                    debug!("batch full event");
                    if let Err(e) = self.flush(false).await {
                        error!("Failed to flush producer: {}", e);
                        self.set_error(e).await;

                        continue;
                    }
                }

                _ = self.batch_events.listen_new_batch() => {

                    debug!("new batch event");
                    linger_sleep = sleep(self.linger);
                }


                _ = &mut linger_sleep => {
                    debug!("Flushing because linger time was reached");
                    if let Err(e) = self.flush(false).await {
                        error!("Failed to flush producer: {:?}", e);
                        self.set_error(e).await;
                        continue;
                    }
                    linger_sleep = sleep(std::time::Duration::from_secs(1800));
                }
            }
        }
        info!("partition producer end");
    }

    async fn set_error(&self, error: FluvioError) {
        let mut error_handle = self.last_error.write().await;
        *error_handle = Some(ProducerError::Internal(error.to_string()));
    }

    async fn current_leader(&self) -> Result<SpuId> {
        let partition_spec = self
            .spu_pool
            .metadata
            .partitions()
            .lookup_by_key(&self.replica)
            .await?
            .ok_or_else(|| {
                FluvioError::PartitionNotFound(
                    self.replica.topic.to_string(),
                    self.replica.partition,
                )
            })?
            .spec;
        Ok(partition_spec.leader)
    }

    /// Flush all the batches that are full or have reached the linger time.
    /// If force is set to true, flush all batches regardless of linger time.
    pub(crate) async fn flush(&self, force: bool) -> Result<()> {
        let leader = self.current_leader().await?;

        let spu_socket = self
            .spu_pool
            .create_serial_socket_from_leader(leader)
            .await?;

        let mut batches_ready = vec![];
        let mut batches = self.batches_lock.lock().await;
        while !batches.is_empty() {
            let ready = force
                || batches.front().map_or(false, |batch| {
                    batch.is_full() || batch.create_time().elapsed() >= self.linger
                });
            if ready {
                if let Some(batch) = batches.pop_front() {
                    batches_ready.push(batch);
                }
            } else {
                break;
            }
        }

        // Send each batch and notify base offset
        let mut request = DefaultProduceRequest::default();

        let mut topic_request = DefaultTopicRequest {
            name: self.replica.topic.to_string(),
            ..Default::default()
        };
        let mut partition_request = DefaultPartitionRequest {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        let mut batch_notifiers = vec![];

        for p_batch in batches_ready {
            let batch = p_batch.records;

            let batch = Batch::from(batch);
            partition_request.records.batches.push(batch);
            batch_notifiers.push(p_batch.notify);
        }

        topic_request.partitions.push(partition_request);
        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);
        let response = spu_socket.send_receive(request).await?;

        for (batch_notifier, response) in batch_notifiers.into_iter().zip(response.responses.iter())
        {
            let base_offset = response.partitions[0].base_offset;
            if let Err(_e) = batch_notifier.send(base_offset).await {
                trace!(
                    "Failed to notify producer of successful produce because receiver was dropped"
                );
            }
        }

        Ok(())
    }
}
