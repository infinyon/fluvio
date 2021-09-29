use std::sync::Arc;
use std::collections::HashMap;
use futures_util::StreamExt;
use flume::{Receiver, Sender};

use tracing::{debug_span, debug, error, instrument};
use tracing_futures::Instrument;
use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::{Result, ProducerError};
use crate::producer::{DispatcherMsg, ProducerConfig, ProducerMsg};
use crate::producer::assoc::{AssociatedResponse, AssociatedRequest, AssociatedRecord, BatchStatus};
use crate::producer::buffer::RecordBuffer;

pub(crate) struct Dispatcher {
    /// Configurations that tweak the dispatcher's behavior.
    config: Arc<ProducerConfig>,
    /// A pool of `PartitionProducer`s that handle each partition's buffer
    partition_pool: PartitionPool,
    /// A stream of events that the dispatcher needs to react to.
    commands: Option<Receiver<DispatcherMsg>>,
    /// A shutdown event used to determine when to quit.
    ///
    /// This will be triggered by the [`Producer`] that starts this dispatcher.
    shutdown: StickyEvent,
}

enum Event {
    Command(DispatcherMsg),
    Timeout,
}

impl Dispatcher {
    pub(crate) fn new(
        spus: Arc<SpuPool>,
        broadcast_status: tokio::sync::broadcast::Sender<BatchStatus>,
        commands: Receiver<DispatcherMsg>,
        config: Arc<ProducerConfig>,
    ) -> Self {
        let shutdown = StickyEvent::new();
        let partition_pool = PartitionPool::new(spus, config.clone(), broadcast_status.clone());

        Self {
            config,
            partition_pool,
            commands: Some(commands),
            shutdown,
        }
    }

    /// Spawn this dispatcher as a task, returning a shutdown handle.
    pub(crate) fn start(self) -> StickyEvent {
        let shutdown = self.shutdown.clone();
        fluvio_future::task::spawn(self.run());
        shutdown
    }

    /// Drive this dispatcher via an event loop that reacts to events.
    pub(crate) async fn run(mut self) {
        // Set up the event stream: Select between new records and timeouts
        let mut events = {
            let timer = async_timer::interval(self.config.batch_duration).map(|_| Event::Timeout);
            let incoming = self.commands.take().unwrap();
            let records = incoming.into_stream().map(Event::Command);
            let select = futures_util::stream::select(timer, records);
            select.take_until(self.shutdown.listen_pinned())
        };

        while let Some(event) = events.next().await {
            let result = self.handle_event(event).await;
            if let Err(e) = result {
                error!("Producer Dispatcher error: {}", e);
            }
        }

        debug!("Producer dispatcher shutting down");
    }

    #[instrument(name = "producer_dispatcher", level = "debug", skip(self, event))]
    async fn handle_event(&mut self, event: Event) -> Result<(), FluvioError> {
        match event {
            Event::Command(DispatcherMsg::Record {
                record,
                respond_to_producer,
            }) => {
                self.partition_pool
                    .buffer(record, respond_to_producer)
                    .await?;
            }
            Event::Command(DispatcherMsg::Flush(respond_to_producer)) => {
                let span = debug_span!("flush", reason = "user_flush");
                self.partition_pool.flush_all().instrument(span).await?;

                // Dropping the sender causes rx.recv() to yield with Err.
                // This itself is used as the message, and ensures exactly one notification
                drop(respond_to_producer);
            }
            Event::Timeout => {
                let span = debug_span!("flush", reason = "timeout");
                self.partition_pool.flush_all().instrument(span).await?;
            }
        }

        Ok(())
    }
}

pub(crate) struct PartitionPool {
    /// Pool for communicating with SPUs.
    spus: Arc<SpuPool>,
    /// Config for producer/flushing behavior
    config: Arc<ProducerConfig>,
    /// A channel for sending the status of batches
    broadcast_status: tokio::sync::broadcast::Sender<BatchStatus>,
    /// Pool of individual `PartitionProducer`s, which oct independently
    pool: HashMap<ReplicaKey, PartitionProducer>,
}

impl PartitionPool {
    /// Create a new `PartitionPool`
    pub fn new(
        spus: Arc<SpuPool>,
        config: Arc<ProducerConfig>,
        broadcast_status: tokio::sync::broadcast::Sender<BatchStatus>,
    ) -> Self {
        Self {
            spus,
            config,
            pool: Default::default(),
            broadcast_status,
        }
    }

    /// Buffer the given record by sending it to the appropriate `PartitionProducer`.
    ///
    /// If a `PartitionProducer` does not exist for this record's `ReplicaKey`, one
    /// will be created.
    pub async fn buffer(
        &mut self,
        record: AssociatedRecord,
        respond_to_caller: Sender<ProducerMsg>,
    ) -> Result<(), FluvioError> {
        let partition_producer = match self.pool.get_mut(&record.replica_key) {
            Some(partition_producer) => partition_producer,
            None => {
                let partition_producer = PartitionProducer::new(
                    self.spus.clone(),
                    self.config.clone(),
                    record.replica_key.clone(),
                    self.broadcast_status.clone(),
                );
                self.pool
                    .insert(record.replica_key.clone(), partition_producer);
                self.pool
                    .get_mut(&record.replica_key)
                    .expect("just inserted, element must exist")
            }
        };

        partition_producer.buffer(record, respond_to_caller).await?;
        Ok(())
    }

    /// Flush the buffers for all partitions.
    pub async fn flush_all(&mut self) -> Result<(), FluvioError> {
        let mut flush_futures = Vec::with_capacity(self.pool.len());
        for (_, partition_producer) in self.pool.iter_mut() {
            let fut = partition_producer.flush();
            flush_futures.push(fut);
        }

        // Flush all concurrently
        let results = futures_util::future::join_all(flush_futures).await;
        for result in results {
            if let Err(e) = result {
                error!("Internal error broadcasting batch status: {}", e);
            }
        }
        Ok(())
    }
}

/// Manages a buffer for a single partition's records.
pub(crate) struct PartitionProducer {
    /// Pool for communicating with the SPUs.
    spus: Arc<SpuPool>,
    /// Producer behavior configuration.
    config: Arc<ProducerConfig>,
    /// This partition's buffered records.
    buffer: RecordBuffer,
    /// A channel for sending the status of batches back to caller.
    broadcast_status: tokio::sync::broadcast::Sender<BatchStatus>,
    /// The topic and partition this producer operates for.
    replica_key: ReplicaKey,
}

impl PartitionProducer {
    /// Create a new `PartitionProducer`.
    pub fn new(
        spus: Arc<SpuPool>,
        config: Arc<ProducerConfig>,
        replica_key: ReplicaKey,
        broadcast_status: tokio::sync::broadcast::Sender<BatchStatus>,
    ) -> Self {
        let batch_size = config.batch_size;
        Self {
            spus,
            config,
            buffer: RecordBuffer::new(batch_size),
            replica_key,
            broadcast_status,
        }
    }

    /// Buffer the given record, flushing if necessary.
    pub async fn buffer(
        &mut self,
        record: AssociatedRecord,
        respond_to_caller: Sender<ProducerMsg>,
    ) -> Result<(), FluvioError> {
        // If this record is too large for the buffer, return it to the producer.
        if !self.buffer.could_fit(&record) {
            respond_to_caller
                .send_async(ProducerMsg::RecordTooLarge(record, self.config.batch_size))
                .await
                .map_err(|_| FluvioError::Other("failed to return oversized record".to_string()))?;
            return Ok(());
        }

        // Try to push the record into the buffer. If this fails, the record is returned in Err
        let buffer_result = self.buffer.push(record);

        // If the record could not be buffered, we need to flush first
        if let Err(bounced) = buffer_result {
            self.flush().await?;

            // After being flushed, the buffer should have room to accept this record.
            // If it did not, we should have returned above at "if !buffer.could_fit"
            self.buffer
                .push(bounced)
                .expect("logic error: Buffer should have room for record");
        }

        drop(respond_to_caller);
        Ok(())
    }

    /// Flush all records from this partition's buffer.
    #[instrument(level = "debug", skip(self))]
    async fn flush(&mut self) -> Result<(), FluvioError> {
        let mut pending_request = AssociatedRequest::new();

        debug!(
            "Flushing {} records ({} bytes)",
            self.buffer.len(),
            self.buffer.size()
        );
        while let Some(record) = self.buffer.pop() {
            pending_request.add(record);
        }

        let result = self.flush_impl(pending_request).await;
        self.broadcast_result(result)?;

        Ok(())
    }

    /// Internal flush, all the methods that might return `Err` for flushing.
    async fn flush_impl(
        &mut self,
        request: AssociatedRequest,
    ) -> Result<AssociatedResponse, FluvioError> {
        let socket = self.spus.create_serial_socket(&self.replica_key).await?;
        let response = socket.send_receive(request.request).await?;
        let associated_response = AssociatedResponse::new(response, request.uids)?;
        Ok(associated_response)
    }

    /// For all batches sent, broadcast their statuses for any callers listening.
    fn broadcast_result(
        &self,
        result: Result<AssociatedResponse, FluvioError>,
    ) -> Result<(), ProducerError> {
        match result {
            Ok(response) => {
                for success in response.successes {
                    self.broadcast_status.send(BatchStatus::Success(success))?;
                }
                for failure in response.failures {
                    self.broadcast_status.send(BatchStatus::Failure(failure))?;
                }
            }
            Err(e) => {
                error!("Error flushing record: {}", e);
                self.broadcast_status
                    .send(BatchStatus::InternalError(e.to_string()))?;
            }
        }

        Ok(())
    }
}
