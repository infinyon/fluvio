use std::sync::Arc;
use std::collections::VecDeque;

use async_lock::{Mutex, RwLock};
use dataplane::ReplicaKey;
use dataplane::batch::{RawRecords, Batch};
use dataplane::produce::{DefaultPartitionRequest, DefaultTopicRequest, DefaultProduceRequest};
use fluvio_future::timer::sleep;
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use tracing::{debug, info, instrument, error, trace};

use crate::error::{Result, FluvioError};
use crate::spu::SpuPool;
use crate::{TopicProducerConfig};

use super::ProducerError;
use super::accumulator::{ProducerBatch, BatchEvents};
use super::event::EventHandler;

#[cfg(feature = "stats")]
use crate::stats::{
    ClientStats, ClientStatsUpdateBuilder, ClientStatsDataCollect, ClientStatsMetricRaw,
};

/// Struct that is responsible for sending produce requests to the SPU in a given partition.
pub(crate) struct PartitionProducer {
    config: Arc<TopicProducerConfig>,
    replica: ReplicaKey,
    spu_pool: Arc<SpuPool>,
    batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
    batch_events: Arc<BatchEvents>,
    last_error: Arc<RwLock<Option<ProducerError>>>,
    #[cfg(feature = "stats")]
    client_stats: Arc<ClientStats>,
}

impl PartitionProducer {
    fn new(
        config: Arc<TopicProducerConfig>,
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        last_error: Arc<RwLock<Option<ProducerError>>>,
        #[cfg(feature = "stats")] client_stats: Arc<ClientStats>,
    ) -> Self {
        Self {
            config,
            replica,
            spu_pool,
            batches_lock,
            batch_events,
            last_error,
            #[cfg(feature = "stats")]
            client_stats,
        }
    }

    pub fn shared(
        config: Arc<TopicProducerConfig>,
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        error: Arc<RwLock<Option<ProducerError>>>,
        #[cfg(feature = "stats")] client_stats: Arc<ClientStats>,
    ) -> Arc<Self> {
        Arc::new(PartitionProducer::new(
            config,
            replica,
            spu_pool,
            batches,
            batch_events,
            error,
            #[cfg(feature = "stats")]
            client_stats,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        config: Arc<TopicProducerConfig>,
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        error: Arc<RwLock<Option<ProducerError>>>,
        end_event: Arc<StickyEvent>,
        flush_event: (Arc<EventHandler>, Arc<EventHandler>),
        #[cfg(feature = "stats")] client_stats: Arc<ClientStats>,
    ) {
        let producer = PartitionProducer::shared(
            config,
            replica,
            spu_pool,
            batches,
            batch_events,
            error,
            #[cfg(feature = "stats")]
            client_stats,
        );
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

        let mut linger_sleep = None;

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
                    }
                    flush_event.1.notify().await;
                    linger_sleep = None;

                }
                _ =  self.batch_events.listen_batch_full() => {
                    debug!("batch full event");
                    if let Err(e) = self.flush(false).await {
                        error!("Failed to flush producer: {}", e);
                        self.set_error(e).await;
                    }
                }

                _ = self.batch_events.listen_new_batch() => {
                    debug!("new batch event");
                    linger_sleep = Some(sleep(self.config.linger));
                }

                _ = async { linger_sleep.as_mut().expect("unexpected failure").await }, if linger_sleep.is_some() => {
                    debug!("Flushing because linger time was reached");

                    if let Err(e) = self.flush(false).await {
                        error!("Failed to flush producer: {:?}", e);
                        self.set_error(e).await;
                    }
                    linger_sleep = None;
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
                    batch.is_full() || batch.elapsed() as u128 >= self.config.linger.as_millis()
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

        let mut batch_notifiers = vec![];

        #[cfg(feature = "stats")]
        let mut client_stats_update = ClientStatsUpdateBuilder::default();
        #[cfg(feature = "stats")]
        let mut total_batch_len = 0;
        #[cfg(feature = "stats")]
        client_stats_update.push(ClientStatsMetricRaw::Batches(batches_ready.len() as u64));

        for p_batch in batches_ready {
            let mut partition_request = DefaultPartitionRequest {
                partition_index: self.replica.partition,
                ..Default::default()
            };
            let notify = p_batch.notify.clone();
            let batch = p_batch.batch();

            let raw_batch: Batch<RawRecords> = batch.try_into()?;

            #[cfg(feature = "stats")]
            // If we are collecting stats, then record
            // the size of all batches about to be sent
            if self.client_stats.is_collect(ClientStatsDataCollect::Data) {
                total_batch_len += raw_batch.batch_len() as u64;
                client_stats_update.push(ClientStatsMetricRaw::Bytes(raw_batch.batch_len() as u64));
                client_stats_update
                    .push(ClientStatsMetricRaw::Records(raw_batch.records_len() as u64));
            }

            partition_request.records.batches.push(raw_batch);
            batch_notifiers.push(notify);
            topic_request.partitions.push(partition_request);
        }

        request.isolation = self.config.isolation;
        request.timeout = self.config.timeout;
        request.topics.push(topic_request);

        #[cfg(feature = "stats")]
        let response = if self.client_stats.is_collect(ClientStatsDataCollect::Data) {
            let (response, send_latency) = self
                .client_stats
                .send_and_measure_latency(&spu_socket, request, total_batch_len)
                .await?;

            client_stats_update += send_latency;
            response
        } else {
            spu_socket.send_receive(request).await?
        };
        #[cfg(not(feature = "stats"))]
        let response = spu_socket.send_receive(request).await?;
        #[cfg(feature = "stats")]
        let mut last_base_offset = 0;
        for (batch_notifier, partition_response) in batch_notifiers.into_iter().zip(
            response
                .responses
                .into_iter()
                .flat_map(|response| response.partitions),
        ) {
            let base_offset = partition_response.base_offset;
            let fluvio_error = partition_response.error_code.clone();

            if let Err(_e) = batch_notifier
                .send((base_offset, fluvio_error.clone()))
                .await
            {
                trace!("Failed to notify produce result because receiver was dropped");
            }

            if fluvio_error.is_error() {
                return Err(FluvioError::from(ProducerError::from(fluvio_error)));
            }

            #[cfg(feature = "stats")]
            {
                last_base_offset = base_offset;
            }
        }

        #[cfg(feature = "stats")]
        // Commit stats update
        if self.client_stats.is_collect(ClientStatsDataCollect::Data) {
            client_stats_update.push(ClientStatsMetricRaw::Offset(last_base_offset as i32));
            self.client_stats.update_batch(client_stats_update).await?
        }

        Ok(())
    }
}
