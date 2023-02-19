use std::time::Instant;

use async_channel::Sender;
use anyhow::Result;

use fluvio::{TopicProducer, RecordKey, Fluvio, TopicProducerConfigBuilder};

use crate::{
    benchmark_config::{
        BenchmarkConfig,
        benchmark_matrix::{RecordKeyAllocationStrategy, SHARED_KEY},
    },
    BenchmarkRecord, generate_random_string, BenchmarkError,
    stats_collector::StatsCollectorMessage,
};

pub struct ProducerWorker {
    fluvio_producer: TopicProducer,
    records_to_send: Option<Vec<BenchmarkRecord>>,
    config: BenchmarkConfig,
    producer_id: u64,
    tx_to_stats_collector: Sender<StatsCollectorMessage>,
}
impl ProducerWorker {
    pub async fn new(
        producer_id: u64,
        config: BenchmarkConfig,
        tx_to_stats_collector: Sender<StatsCollectorMessage>,
    ) -> Result<Self> {
        let fluvio = Fluvio::connect().await?;

        let fluvio_config = TopicProducerConfigBuilder::default()
            .batch_size(config.producer_batch_size as usize)
            .batch_queue_size(config.producer_queue_size as usize)
            .linger(config.producer_linger)
            // todo allow alternate partitioner
            .compression(config.producer_compression)
            .timeout(config.producer_server_timeout)
            .isolation(config.producer_isolation)
            .delivery_semantic(config.producer_delivery_semantic)
            .build()
            .map_err(|e| {
                BenchmarkError::ErrorWithExplanation(format!("Fluvio topic config error: {e:?}"))
            })?;
        let fluvio_producer = fluvio
            .topic_producer_with_config(config.topic_name.clone(), fluvio_config)
            .await?;
        Ok(ProducerWorker {
            fluvio_producer,
            records_to_send: None,
            config,
            producer_id,
            tx_to_stats_collector,
        })
    }
    pub async fn prepare_for_batch(&mut self) {
        let records = (0..self.config.num_records_per_producer_worker_per_batch)
            .map(|i| {
                let key = match self.config.record_key_allocation_strategy {
                    RecordKeyAllocationStrategy::NoKey => RecordKey::NULL,
                    RecordKeyAllocationStrategy::AllShareSameKey => RecordKey::from(SHARED_KEY),
                    RecordKeyAllocationStrategy::ProducerWorkerUniqueKey => {
                        RecordKey::from(format!("producer-{}", self.producer_id.clone()))
                    }
                    RecordKeyAllocationStrategy::RoundRobinKey(x) => {
                        RecordKey::from(format!("rr-{}", i % x))
                    }
                    RecordKeyAllocationStrategy::RandomKey => {
                        RecordKey::from(format!("random-{}", generate_random_string(10)))
                    }
                };
                let data = generate_random_string(self.config.record_size as usize);
                BenchmarkRecord::new(key, data)
            })
            .collect();
        self.records_to_send = Some(records);
    }

    pub async fn send_batch(&mut self) -> Result<()> {
        for record in self.records_to_send.take().ok_or_else(|| {
            BenchmarkError::ErrorWithExplanation(
                "prepare_for_batch() not called on PrdoucerWorker".to_string(),
            )
        })? {
            self.tx_to_stats_collector
                .send(StatsCollectorMessage::MessageSent {
                    hash: record.hash,
                    send_time: Instant::now(),
                    num_bytes: record.data.len() as u64,
                })
                .await?;

            self.fluvio_producer.send(record.key, record.data).await?;
        }
        self.fluvio_producer.flush().await?;
        self.tx_to_stats_collector
            .send(StatsCollectorMessage::ProducerFlushed {
                flush_time: Instant::now(),
            })
            .await?;
        Ok(())
    }
}
