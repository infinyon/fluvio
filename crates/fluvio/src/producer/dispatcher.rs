use std::sync::Arc;
use std::collections::HashMap;
use futures_util::StreamExt;
use async_channel::Receiver;

use tracing::{error, debug, trace};
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::batch::Batch;
use dataplane::produce::{ProduceRequest, TopicProduceData, PartitionProduceData, ProduceResponse};
use fluvio_protocol::Encoder;
use fluvio_protocol::api::Request;

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::{PendingRecord, DispatcherMessage, ProducerConfig};
use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner, PartitionerConfig};
use dataplane::record::RecordSet;
use crate::metadata::topic::TopicSpec;
use crate::metadata::partition::PartitionSpec;

pub(crate) struct Dispatcher {
    /// A pool of connections to the SPUs.
    pool: Arc<SpuPool>,
    /// Configurations that tweak the dispatcher's behavior.
    config: ProducerConfig,
    /// The partitioning strategy to use
    partitioner: Box<dyn Partitioner + Send + Sync>,
    /// A buffer of records that have yet to be sent to the cluster.
    buffer: HashMap<SpuId, ProduceRequest>,
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
            let timer = async_timer::interval(self.config.batch_duration).map(|_| Event::Timeout);
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
            Event::ProducerRequest(DispatcherMessage::Record(record)) => {
                let maybe_flush = self.buffer_record(record).await?;
                if let Some((leader, request)) = maybe_flush {
                    self.send_request(leader, request).await?;
                }
            }
            Event::ProducerRequest(DispatcherMessage::Flush(sender)) => {
                self.flush_requests().await?;

                // Dropping the sender causes rx.recv() to yield with Err.
                // This itself is used as the message, and ensures exactly one notification
                drop(sender);
            }
            Event::Timeout => {
                let buffer_sizes = self
                    .buffer
                    .iter()
                    .map(|(leader, request)| {
                        let size = Encoder::write_size(
                            request,
                            ProduceRequest::<RecordSet>::DEFAULT_API_VERSION,
                        );
                        (leader, size)
                    })
                    .collect::<Vec<_>>();
                trace!("Producer flush timeout, buffer: {:?}", buffer_sizes);
                self.flush_requests().await?;
            }
        }

        Ok(())
    }

    /// Retrieves the metadata for a named Topic
    async fn lookup_topic(&self, topic: &String) -> Result<TopicSpec, FluvioError> {
        let topic_spec = self
            .pool
            .metadata
            .topics()
            .lookup_by_key(topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
            .spec;
        Ok(topic_spec)
    }

    /// Retrieves the metadata for a specified Partition
    async fn lookup_partition(
        &self,
        replica_key: &ReplicaKey,
    ) -> Result<PartitionSpec, FluvioError> {
        let partition_spec = self
            .pool
            .metadata
            .partitions()
            .lookup_by_key(&replica_key)
            .await?
            .ok_or_else(|| {
                FluvioError::PartitionNotFound(replica_key.topic.clone(), replica_key.partition)
            })?
            .spec;
        Ok(partition_spec)
    }

    /// Buffer a given record after determining it's partition.
    async fn buffer_record(
        &mut self,
        record: PendingRecord<()>,
    ) -> Result<Option<(SpuId, ProduceRequest)>, FluvioError> {
        let topic_spec = self.lookup_topic(&record.topic).await?;

        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };
        let partition = self.partitioner.partition(
            &partition_config,
            record.record.key.as_ref().map(AsRef::as_ref),
            record.record.value.as_ref(),
        );
        let replica_key = ReplicaKey::new(&record.topic, partition);
        let partition_spec = self.lookup_partition(&replica_key).await?;
        let leader = partition_spec.leader;

        let record = PendingRecord {
            partition,
            topic: record.topic,
            record: record.record,
        };

        let buffered_request = self
            .buffer
            .entry(leader)
            .or_insert_with(|| Default::default());

        // Check the sizes of the buffered request and the new record
        let api_version = ProduceRequest::<RecordSet>::DEFAULT_API_VERSION;
        let batch_size = Encoder::write_size(buffered_request, api_version);
        let record_size = record.record.write_size(api_version);

        // We need to flush if the combined batch size would be bigger than the max size
        let needs_flush = batch_size + record_size > self.config.batch_size;
        trace!(batch_size, record_size, needs_flush);

        // If we need to flush, take the buffered request and replace it with a new request
        let flush_request =
            needs_flush.then(|| std::mem::replace(buffered_request, Default::default()));

        // This adds the new record to whichever is now the "buffered" request.
        //
        // If the batch size was _not_ too big, this will be the same request as before.
        // If the batch size _was_ too big, this will be the new empty request we replaced it with
        add_record_to_request(buffered_request, record);

        let flush = flush_request.map(|it| (leader, it));
        Ok(flush)
    }

    /// Empty all requests from the buffer and send them to their respective SPUs.
    async fn flush_requests(&mut self) -> Result<(), FluvioError> {
        // Take all requests from the buffer, replacing them with empty requests
        let mut requests_to_flush = Vec::with_capacity(self.buffer.len());
        for (&leader, request) in self.buffer.iter_mut() {
            let request = std::mem::replace(request, Default::default());
            requests_to_flush.push((leader, request));
        }

        // Set up futures for sending requests to SPUs concurrently
        let mut response_futures = Vec::with_capacity(requests_to_flush.len());
        for (leader, request) in requests_to_flush {
            let future = self.send_request(leader, request);
            response_futures.push(future);
        }

        // Send the requests to all SPUs concurrently.
        let responses = futures_util::future::join_all(response_futures).await;
        for result in responses {
            if let Err(err) = result {
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

fn add_record_to_request(request: &mut ProduceRequest, record: PendingRecord) {
    let topic = {
        let maybe_topic = request.topics.iter_mut().find(|t| t.name == record.topic);
        match maybe_topic {
            Some(t) => t,
            None => {
                let topic = TopicProduceData {
                    name: record.topic.clone(),
                    ..Default::default()
                };
                request.topics.push(topic);
                request.topics.last_mut().unwrap()
            }
        }
    };

    let partition = {
        let maybe_partition = topic
            .partitions
            .iter_mut()
            .find(|p| p.partition_index == record.partition);
        match maybe_partition {
            Some(p) => p,
            None => {
                let partition = PartitionProduceData {
                    partition_index: record.partition,
                    ..Default::default()
                };
                topic.partitions.push(partition);
                topic.partitions.last_mut().unwrap()
            }
        }
    };

    let batch = {
        let maybe_batch = partition.records.batches.first_mut();
        match maybe_batch {
            Some(b) => b,
            None => {
                partition.records.batches.push(Batch::new());
                partition.records.batches.last_mut().unwrap()
            }
        }
    };

    batch.add_record(record.record);
}
