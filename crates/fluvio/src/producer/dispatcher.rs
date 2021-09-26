use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use futures_util::StreamExt;
use async_channel::{Receiver, Sender};

use tracing::{error, debug, trace};
use fluvio_types::SpuId;
use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::batch::Batch;
use dataplane::record::{RecordSet, Record};
use dataplane::produce::{ProduceRequest, TopicProduceData, PartitionProduceData};
use fluvio_protocol::Encoder;
use fluvio_protocol::api::Request;

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::{DispatcherMsg, ProducerConfig, ProducerMsg, PendingRecord};
use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};

pub(crate) struct Dispatcher {
    /// A pool of connections to the SPUs.
    pool: Arc<SpuPool>,
    /// Configurations that tweak the dispatcher's behavior.
    config: ProducerConfig,
    /// The partitioning strategy to use
    partitioner: Box<dyn Partitioner + Send + Sync>,
    /// A buffer of records that have yet to be sent to the cluster.
    buffer: HashMap<SpuId, ProduceRequest>,
    bufferz: HashMap<ReplicaKey, RecordBuffer>,
    /// A stream of events that the dispatcher needs to react to.
    incoming: Option<Receiver<DispatcherMsg>>,
    /// A shutdown event used to determine when to quit.
    ///
    /// This will be triggered by the [`Producer`] that starts this dispatcher.
    shutdown: StickyEvent,
}

#[derive(Debug)]
pub(crate) struct RecordBuffer {
    size: usize,
    max_size: usize,
    records: VecDeque<Record>,
}

impl RecordBuffer {
    pub fn new(max_size: usize) -> Self {
        Self {
            size: 0,
            max_size,
            records: VecDeque::new(),
        }
    }

    pub fn push(&mut self, record: Record) -> Result<(), Record> {
        let record_size =
            Encoder::write_size(&record, ProduceRequest::<RecordSet>::DEFAULT_API_VERSION);

        if self.size + record_size > self.max_size {
            return Err(record);
        }

        self.records.push_back(record);
        self.size += record_size;

        Ok(())
    }

    pub fn pop(&mut self) -> Option<Record> {
        let record = self.records.pop_front();
        match record {
            None => None,
            Some(record) => {
                let record_size =
                    Encoder::write_size(&record, ProduceRequest::<RecordSet>::DEFAULT_API_VERSION);
                self.size.saturating_sub(record_size);
                Some(record)
            }
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

enum Event {
    Command(DispatcherMsg),
    Timeout,
}

impl Dispatcher {
    pub(crate) fn new(
        pool: Arc<SpuPool>,
        incoming: Receiver<DispatcherMsg>,
        config: ProducerConfig,
    ) -> Self {
        let partitioner = Box::new(SiphashRoundRobinPartitioner::new());
        let shutdown = StickyEvent::new();

        Self {
            pool,
            config,
            partitioner,
            buffer: Default::default(),
            bufferz: Default::default(),
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
            let records = incoming.map(Event::Command);
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
            Event::Command(DispatcherMsg::Record {
                record,
                to_producer,
            }) => {
                self.try_buffer(record, to_producer).await?;
            }
            Event::Command(DispatcherMsg::Flush(sender)) => {
                print!("Manual flush: ");
                self.flush_all().await?;

                // Dropping the sender causes rx.recv() to yield with Err.
                // This itself is used as the message, and ensures exactly one notification
                drop(sender);
            }
            Event::Timeout => {
                print!("Timeout: ");
                self.flush_all().await?;
            }
        }

        Ok(())
    }

    async fn try_buffer(
        &mut self,
        mut record: PendingRecord,
        to_producer: Sender<ProducerMsg>,
    ) -> Result<(), FluvioError> {
        let batch_size = self.config.batch_size;
        let buffer = self
            .bufferz
            .entry(record.replica_key.clone())
            .or_insert_with(|| RecordBuffer::new(batch_size));

        // Try to push the record into the buffer. If this fails, the record is returned in Err
        let buffer_result = buffer.push(record.record);

        // If the record could not be buffered, send it back to producer
        if let Err(bounced) = buffer_result {
            let mut buffer_to_flush =
                std::mem::replace(buffer, RecordBuffer::new(self.config.batch_size));
            print!("Size overflow: ");
            flush_buffer(self.pool.clone(), &record.replica_key, &mut buffer_to_flush).await?;

            record.record = bounced;
            to_producer
                .send(ProducerMsg::BufferFull(record))
                .await
                .map_err(|_| {
                    FluvioError::Other("failed to return record to producer".to_string())
                })?;
        }

        drop(to_producer);
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), FluvioError> {
        let mut result_futures = Vec::with_capacity(self.bufferz.len());
        for (key, buffer) in self.bufferz.iter_mut() {
            let fut = flush_buffer(self.pool.clone(), key, buffer);
            result_futures.push(fut);
        }

        let results = futures_util::future::join_all(result_futures).await;
        for result in results {
            if let Err(e) = result {
                error!("Error flushing record: {}", e);
            }
        }
        Ok(())
    }
}

fn add_record_to_request(request: &mut ProduceRequest, key: &ReplicaKey, record: Record) {
    let topic = {
        let maybe_topic = request.topics.iter_mut().find(|t| t.name == key.topic);
        match maybe_topic {
            Some(t) => t,
            None => {
                let topic = TopicProduceData {
                    name: key.topic.clone(),
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
            .find(|p| p.partition_index == key.partition);
        match maybe_partition {
            Some(p) => p,
            None => {
                let partition = PartitionProduceData {
                    partition_index: key.partition,
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

    batch.add_record(record);
}

async fn flush_buffer(
    spus: Arc<SpuPool>,
    key: &ReplicaKey,
    buffer: &mut RecordBuffer,
) -> Result<(), FluvioError> {
    let mut request: ProduceRequest = Default::default();

    println!(
        "Flushing {} records ({} bytes)",
        buffer.len(),
        buffer.size()
    );
    while let Some(record) = buffer.pop() {
        add_record_to_request(&mut request, key, record);
    }

    let socket = spus.create_serial_socket(key).await?;
    let response = socket.send_receive(request).await?;

    Ok(())
}
