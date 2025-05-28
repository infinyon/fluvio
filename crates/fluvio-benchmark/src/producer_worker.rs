use std::sync::Arc;

use anyhow::Result;

use flume::Sender;
use fluvio::{
    dataplane::record::RecordData, DeliverySemantic, Fluvio, Isolation,
    ProduceCompletionBatchEvent, ProducerCallback, SharedProducerCallback, RecordKey,
    TopicProducerConfigBuilder, TopicProducerPool,
};
use futures_util::future::BoxFuture;
use tracing::debug;

use crate::{
    config::{ProducerConfig, RecordKeyAllocationStrategy},
    utils,
};

const SHARED_KEY: &str = "shared_key";

// Example implementation of the ProducerCallback trait
#[derive(Debug)]
struct BenchmarkProducerCallback {
    event_sender: Sender<ProduceCompletionBatchEvent>,
}

impl BenchmarkProducerCallback {
    pub fn new(event_sender: Sender<ProduceCompletionBatchEvent>) -> Self {
        Self { event_sender }
    }
}

impl ProducerCallback for BenchmarkProducerCallback {
    fn finished(&self, event: ProduceCompletionBatchEvent) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            self.event_sender.send_async(event).await?;
            Ok(())
        })
    }
}

pub(crate) struct ProducerWorker {
    fluvio_producer: TopicProducerPool,
    records_to_send: Vec<BenchmarkRecord>,
}
impl ProducerWorker {
    pub(crate) async fn new(
        id: u64,
        config: ProducerConfig,
        event_sender: Sender<ProduceCompletionBatchEvent>,
    ) -> Result<Self> {
        let fluvio = Fluvio::connect().await?;
        let callback: SharedProducerCallback =
            Arc::new(BenchmarkProducerCallback::new(event_sender));

        let fluvio_config = TopicProducerConfigBuilder::default()
            .callback(callback)
            .batch_size(config.batch_size.as_u64() as usize)
            .batch_queue_size(config.queue_size as usize)
            .max_request_size(config.max_request_size.as_u64() as usize)
            .linger(config.linger)
            .compression(config.compression)
            .timeout(config.server_timeout)
            .isolation(Isolation::ReadUncommitted)
            .delivery_semantic(DeliverySemantic::default())
            .build()?;

        let fluvio_producer = fluvio
            .topic_producer_with_config(config.topic_name.clone(), fluvio_config)
            .await?;

        let num_records = utils::records_per_producer(id, config.num_producers, config.num_records);

        let records_to_send = create_records(config.clone(), num_records, id);

        Ok(ProducerWorker {
            fluvio_producer,
            records_to_send,
        })
    }

    pub async fn send_batch(self) -> Result<()> {
        debug!("producer is sending batch");

        for record in self.records_to_send.into_iter() {
            let _ = self
                .fluvio_producer
                .send(record.key, record.data.clone())
                .await?;
        }
        self.fluvio_producer.flush().await?;

        Ok(())
    }
}

fn create_records(config: ProducerConfig, num_records: u64, id: u64) -> Vec<BenchmarkRecord> {
    utils::generate_random_string_vec(num_records as usize, config.record_size.as_u64() as usize)
        .into_iter()
        .map(|data| {
            let key = match config.record_key_allocation_strategy {
                RecordKeyAllocationStrategy::NoKey => RecordKey::NULL,
                RecordKeyAllocationStrategy::AllShareSameKey => RecordKey::from(SHARED_KEY),
                RecordKeyAllocationStrategy::ProducerWorkerUniqueKey => {
                    RecordKey::from(format!("producer-{}", id.clone()))
                }
                RecordKeyAllocationStrategy::RandomKey => {
                    //TODO: this could be optimized
                    RecordKey::from(format!("random-{}", utils::generate_random_string(10)))
                }
            };
            BenchmarkRecord::new(key, data.into())
        })
        .collect()
}

pub struct BenchmarkRecord {
    pub key: RecordKey,
    pub data: RecordData,
}

impl BenchmarkRecord {
    pub fn new(key: RecordKey, data: RecordData) -> Self {
        Self { key, data }
    }
}
