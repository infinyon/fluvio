use std::sync::Arc;
use std::collections::HashMap;
use futures_util::StreamExt;
use async_channel::Receiver;

use tracing::{error, debug};
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::batch::Batch;
use dataplane::produce::{ProduceRequest, TopicProduceData, PartitionProduceData, ProduceResponse};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::{PendingRecord, DispatcherMessage, ProducerConfig};
use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner, PartitionerConfig};

pub(crate) struct Dispatcher {
    /// A pool of connections to the SPUs.
    pool: Arc<SpuPool>,
    /// Configurations that tweak the dispatcher's behavior.
    config: ProducerConfig,
    /// The partitioning strategy to use
    partitioner: Box<dyn Partitioner + Send + Sync>,
    /// A buffer of records that have yet to be sent to the cluster.
    buffer: Vec<PendingRecord>,
    /// A stream of events that the dispatcher needs to react to.
    incoming: Option<Receiver<DispatcherMessage>>,
    /// A shutdown event used to determine when to quit.
    ///
    /// This will be triggered by the [`Producer`] that starts this dispatcher.
    shutdown: StickyEvent,
}

enum Event {
    ProducerRequest(DispatcherMessage),
    Timeout,
}

impl Dispatcher {
    pub(crate) fn new(
        pool: Arc<SpuPool>,
        incoming: Receiver<DispatcherMessage>,
        config: ProducerConfig,
    ) -> Self {
        let partitioner = Box::new(SiphashRoundRobinPartitioner::new());
        let shutdown = StickyEvent::new();

        Self {
            pool,
            config,
            partitioner,
            buffer: Default::default(),
            incoming: Some(incoming),
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
            let timer =
                async_timer::interval(std::time::Duration::from_millis(10)).map(|_| Event::Timeout);
            let incoming = self.incoming.take().unwrap();
            let records = incoming.map(Event::ProducerRequest);
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

    async fn handle_event(&mut self, event: Event) -> Result<(), FluvioError> {
        match event {
            Event::ProducerRequest(DispatcherMessage::Record(record_pending)) => {
                self.buffer_record(record_pending).await?;

                // TODO check buffer capacity, maybe flush
            }
            Event::ProducerRequest(DispatcherMessage::Flush(sender)) => {
                self.flush_buffer().await?;

                // TODO actually handle
                sender.send(()).await.unwrap();
            }
            Event::Timeout => {
                debug!("Producer flush timeout, buffer size: {}", self.buffer.len());
                self.flush_buffer().await?;
            }
        }

        Ok(())
    }

    /// Buffer a given record after determining it's partition.
    async fn buffer_record(&mut self, record: PendingRecord<()>) -> Result<(), FluvioError> {
        let topics = self.pool.metadata.topics();
        let topic_spec = topics
            .lookup_by_key(&record.topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(record.topic.to_string()))?
            .spec;

        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };
        let partition = self.partitioner.partition(
            &partition_config,
            record.record.key.as_ref().map(AsRef::as_ref),
            record.record.value.as_ref(),
        );

        let record = PendingRecord {
            partition,
            topic: record.topic,
            record: record.record,
        };

        self.buffer.push(record);
        Ok(())
    }

    /// Flush the buffered records to the appropriate SPUs.
    async fn flush_buffer(&mut self) -> Result<(), FluvioError> {
        let mut by_spu: HashMap<SpuId, ProduceRequest> = HashMap::new();
        let partition_store = self.pool.metadata.partitions();

        if self.buffer.is_empty() {
            return Ok(());
        }

        for pending in self.buffer.drain(..) {
            let replica_key = ReplicaKey::new(&pending.topic, pending.partition);
            let partition = partition_store
                .lookup_by_key(&replica_key)
                .await?
                .ok_or_else(|| {
                    FluvioError::PartitionNotFound(pending.topic.to_string(), pending.partition)
                })?
                .spec;
            let leader = partition.leader;
            let topic = &pending.topic;
            let partition = pending.partition;

            // Initialize the Request and Senders collection for this SPU
            let request = by_spu.entry(leader).or_insert_with(Default::default);

            // Get or create a TopicProduceData for this topic
            let maybe_topic = request.topics.iter_mut().find(|it| &it.name == topic);
            let topic_request = match maybe_topic {
                Some(t) => t,
                None => {
                    let topic = TopicProduceData {
                        name: pending.topic.to_string(),
                        ..Default::default()
                    };
                    request.topics.push(topic);
                    request.topics.last_mut().unwrap()
                }
            };

            // Get or create a PartitionProduceData for this partition
            let maybe_partition = topic_request
                .partitions
                .iter_mut()
                .find(|it| it.partition_index == partition);
            let partition_request = match maybe_partition {
                Some(p) => p,
                None => {
                    let partition = PartitionProduceData {
                        partition_index: pending.partition,
                        ..Default::default()
                    };
                    topic_request.partitions.push(partition);
                    topic_request.partitions.last_mut().unwrap()
                }
            };

            // Get or create a Batch in this PartitionProduceData
            let maybe_batch = partition_request.records.batches.first_mut();
            let batch = match maybe_batch {
                Some(b) => b,
                None => {
                    partition_request.records.batches.push(Batch::new());
                    partition_request.records.batches.last_mut().unwrap()
                }
            };

            // Add this record to the batch in the request
            batch.add_record(pending.record);
        }

        // Set up futures for sending requests to SPUs concurrently
        let mut response_futures = Vec::with_capacity(by_spu.len());
        for (leader, request) in by_spu {
            let future = self.send_request(leader, request);
            response_futures.push(future);
        }

        // Send the requests to all SPUs concurrently.
        let responses = futures_util::future::join_all(response_futures).await;
        for response in responses {
            if let Err(err) = response {
                error!("Error flushing records to SPU: {}", err);
            }
        }

        Ok(())
    }

    /// Send the request for a given leader SPU, and collect a Response or Error.
    async fn send_request(
        &self,
        leader: SpuId,
        request: ProduceRequest,
    ) -> Result<ProduceResponse, FluvioError> {
        let socket = self.pool.create_serial_socket_from_leader(leader).await?;
        let response = socket.send_receive(request).await?;
        Ok(response)
    }
}
