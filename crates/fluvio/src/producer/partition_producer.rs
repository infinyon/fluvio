use std::sync::Arc;
use std::collections::VecDeque;

use async_lock::{Mutex, RwLock};
use dataplane::ReplicaKey;
use dataplane::batch::{RawRecords, Batch, BATCH_FILE_HEADER_SIZE};
use dataplane::produce::{
    DefaultPartitionRequest, DefaultTopicRequest, DefaultProduceRequest, ProduceRequest,
    ProduceResponse,
};
use dataplane::record::RecordSet;
use fluvio_future::timer::sleep;
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use tracing::{debug, info, instrument, error, trace};

use crate::error::{Result, FluvioError};
use crate::sockets::VersionedSerialSocket;
use crate::spu::SpuPool;
use crate::TopicProducerConfig;

use super::ProducerError;
use super::accumulator::{ProducerBatch, BatchEvents};
use super::event::EventHandler;

use crate::stats::{ClientStats, ClientStatsUpdate};
use std::time::Instant;

use quantities::prelude::*;
use quantities::datavolume::BYTE;
use quantities::duration::SECOND;

/// Struct that is responsible for sending produce requests to the SPU in a given partition.
pub(crate) struct PartitionProducer {
    config: Arc<TopicProducerConfig>,
    replica: ReplicaKey,
    spu_pool: Arc<SpuPool>,
    batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
    batch_events: Arc<BatchEvents>,
    last_error: Arc<RwLock<Option<ProducerError>>>,
    client_stats: Arc<RwLock<ClientStats>>,
}

impl PartitionProducer {
    fn new(
        config: Arc<TopicProducerConfig>,
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
        batch_events: Arc<BatchEvents>,
        last_error: Arc<RwLock<Option<ProducerError>>>,
        client_stats: Arc<RwLock<ClientStats>>,
    ) -> Self {
        Self {
            config,
            replica,
            spu_pool,
            batches_lock,
            batch_events,
            last_error,
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
        client_stats: Arc<RwLock<ClientStats>>,
    ) -> Arc<Self> {
        Arc::new(PartitionProducer::new(
            config,
            replica,
            spu_pool,
            batches,
            batch_events,
            error,
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
        client_stats: Arc<RwLock<ClientStats>>,
    ) {
        let producer = PartitionProducer::shared(
            config,
            replica,
            spu_pool,
            batches,
            batch_events,
            error,
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

    // This is where I want to add the instrumenting
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

        let mut flush_total_bytes = 0;

        for p_batch in batches_ready {
            let mut partition_request = DefaultPartitionRequest {
                partition_index: self.replica.partition,
                ..Default::default()
            };
            let notify = p_batch.notify.clone();
            let batch = p_batch.batch();

            let raw_batch: Batch<RawRecords> = batch.try_into()?;

            flush_total_bytes += BATCH_FILE_HEADER_SIZE + raw_batch.records().0.len();

            partition_request.records.batches.push(raw_batch);
            batch_notifiers.push(notify);
            topic_request.partitions.push(partition_request);
        }

        request.isolation = self.config.isolation;
        request.timeout = self.config.timeout;
        request.topics.push(topic_request);

        let (response, stats_update) = Self::measure_send_receive(&spu_socket, request).await?;

        for (batch_notifier, response) in batch_notifiers.into_iter().zip(response.responses.iter())
        {
            let base_offset = response.partitions[0].base_offset;
            let fluvio_error = response.partitions[0].error_code.clone();

            if let Err(_e) = batch_notifier
                .send((base_offset, fluvio_error.clone()))
                .await
            {
                trace!("Failed to notify produce result because receiver was dropped");
            }

            if fluvio_error.is_error() {
                return Err(FluvioError::from(ProducerError::from(fluvio_error)));
            }

            // Type conversion for `quantities` crate

            #[cfg(not(target_arch = "wasm32"))]
            let scratch: AmountT = flush_total_bytes as f64;
            #[cfg(target_arch = "wasm32")]
            let scratch: AmountT = flush_total_bytes as f32;

            let flush_total_bytes = Some(scratch * BYTE);

            // Update stats
            let mut stats_handle = self.client_stats.write().await;
            stats_handle.update(stats_update.bytes(flush_total_bytes));
            drop(stats_handle);
        }

        Ok(())
    }

    async fn measure_send_receive(
        socket: &VersionedSerialSocket,
        request: ProduceRequest<RecordSet<RawRecords>>,
    ) -> Result<(ProduceResponse, ClientStatsUpdate)> {
        let send_start_time = Instant::now();

        let response = socket.send_receive(request).await?;

        let send_latency = send_start_time.elapsed();

        #[cfg(not(target_arch = "wasm32"))]
        let scratch = send_latency.as_secs_f64();
        #[cfg(target_arch = "wasm32")]
        let scratch = send_latency.as_secs_f32();

        let send_latency = Some(scratch * SECOND);

        let stats_update = ClientStatsUpdate::new().latency(send_latency);

        Ok((response, stats_update))
    }
}
