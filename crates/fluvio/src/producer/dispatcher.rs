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
    /// A pool of connections to the SPUs.
    pool: Arc<SpuPool>,
    /// Configurations that tweak the dispatcher's behavior.
    config: ProducerConfig,
    /// A buffer of records that have yet to be sent to the cluster.
    buffers: HashMap<ReplicaKey, RecordBuffer>,
    /// A channel for sending the status of batches
    statuses: tokio::sync::broadcast::Sender<BatchStatus>,
    /// A stream of events that the dispatcher needs to react to.
    incoming: Option<Receiver<DispatcherMsg>>,
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
        pool: Arc<SpuPool>,
        statuses: tokio::sync::broadcast::Sender<BatchStatus>,
        commands: Receiver<DispatcherMsg>,
        config: ProducerConfig,
    ) -> Self {
        let shutdown = StickyEvent::new();

        Self {
            pool,
            config,
            buffers: Default::default(),
            statuses,
            incoming: Some(commands),
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
            let incoming = self.incoming.take().unwrap();
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
                to_producer,
            }) => {
                self.try_buffer(record, to_producer).await?;
            }
            Event::Command(DispatcherMsg::Flush(sender)) => {
                let span = debug_span!("flush", reason = "user_flush");
                self.flush_all().instrument(span).await?;

                // Dropping the sender causes rx.recv() to yield with Err.
                // This itself is used as the message, and ensures exactly one notification
                drop(sender);
            }
            Event::Timeout => {
                let span = debug_span!("flush", reason = "timeout");
                self.flush_all().instrument(span).await?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, pending, to_producer))]
    async fn try_buffer(
        &mut self,
        pending: AssociatedRecord,
        to_producer: Sender<ProducerMsg>,
    ) -> Result<(), FluvioError> {
        let batch_size = self.config.batch_size;
        let buffer = self
            .buffers
            .entry(pending.replica_key.clone())
            .or_insert_with(|| RecordBuffer::new(batch_size));

        // If this record is too large for the buffer, return it to the producer.
        if !buffer.could_fit(&pending) {
            to_producer
                .send_async(ProducerMsg::RecordTooLarge(pending, self.config.batch_size))
                .await
                .map_err(|_| FluvioError::Other("failed to return oversized record".to_string()))?;
            return Ok(());
        }

        // Try to push the record into the buffer. If this fails, the record is returned in Err
        let replica_key = pending.replica_key.clone();
        let buffer_result = buffer.push(pending);

        // If the record could not be buffered, we need to flush first
        if let Err(bounced) = buffer_result {
            let mut buffer_to_flush =
                std::mem::replace(buffer, RecordBuffer::new(self.config.batch_size));
            let response =
                flush_buffer(self.pool.clone(), &replica_key, &mut buffer_to_flush).await?;

            // After being flushed, the buffer should have room to accept this record.
            // If it did not, we should have returned above at "if !buffer.could_fit"
            buffer
                .push(bounced)
                .expect("logic error: Buffer should have room for record");

            self.broadcast_response_statuses(response)?;
        }

        drop(to_producer);
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), FluvioError> {
        let mut result_futures = Vec::with_capacity(self.buffers.len());
        for (key, buffer) in self.buffers.iter_mut() {
            let fut = flush_buffer(self.pool.clone(), key, buffer);
            result_futures.push(fut);
        }

        let results: Vec<Result<AssociatedResponse, FluvioError>> =
            futures_util::future::join_all(result_futures).await;
        for result in results {
            let response = match result {
                Ok(response) => response,
                Err(e) => {
                    error!("Error flushing record: {}", e);
                    continue;
                }
            };

            self.broadcast_response_statuses(response)?;
        }
        Ok(())
    }

    fn broadcast_response_statuses(&self, response: AssociatedResponse) -> Result<()> {
        for success in response.successes {
            self.broadcast_status(BatchStatus::Success(success))?;
        }
        for failure in response.failures {
            self.broadcast_status(BatchStatus::Failure(failure))?;
        }

        Ok(())
    }

    fn broadcast_status(&self, status: BatchStatus) -> Result<(), ProducerError> {
        self.statuses.send(status)?;
        Ok(())
    }
}

#[instrument(level = "debug", skip(spus, buffer))]
async fn flush_buffer(
    spus: Arc<SpuPool>,
    key: &ReplicaKey,
    buffer: &mut RecordBuffer,
) -> Result<AssociatedResponse, FluvioError> {
    let mut pending_request = AssociatedRequest::new();

    debug!(
        "Flushing {} records ({} bytes)",
        buffer.len(),
        buffer.size()
    );
    while let Some(record) = buffer.pop() {
        pending_request.add(record);
    }

    let socket = spus.create_serial_socket(key).await?;
    let response = socket.send_receive(pending_request.request).await?;
    let associated_response = AssociatedResponse::new(response, pending_request.uids)?;

    Ok(associated_response)
}
