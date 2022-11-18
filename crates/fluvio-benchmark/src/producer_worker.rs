use std::time::Instant;

use async_std::channel::Sender;
use fluvio::{TopicProducer, RecordKey, Fluvio, TopicProducerConfigBuilder};

use crate::{
    benchmark_config::{
        benchmark_settings::BenchmarkSettings,
        benchmark_matrix::{RecordKeyAllocationStrategy, RecordSizeStrategy, SHARED_KEY},
    },
    BenchmarkRecord, generate_random_string, BenchmarkError,
    stats_collector::StatsCollectorMessage,
};

pub struct ProducerWorker {
    fluvio_producer: TopicProducer,
    records_to_send: Option<Vec<BenchmarkRecord>>,
    settings: BenchmarkSettings,
    producer_id: String,
    tx_to_stats_collector: Sender<StatsCollectorMessage>,
}
impl ProducerWorker {
    pub async fn new(
        settings: BenchmarkSettings,
        tx_to_stats_collector: Sender<StatsCollectorMessage>,
    ) -> Self {
        let fluvio = Fluvio::connect().await.unwrap();

        let config = TopicProducerConfigBuilder::default()
            .batch_size(settings.producer_batch_size as usize)
            .batch_queue_size(settings.producer_queue_size as usize)
            .linger(settings.producer_linger)
            // todo allow alternate partitioner
            .compression(settings.producer_compression)
            .timeout(settings.producer_server_timeout)
            // todo producer isolation
            // todo producer delivery_semantic
            .build()
            .unwrap();
        let fluvio_producer = fluvio
            .topic_producer_with_config(settings.topic_name.clone(), config)
            .await
            .unwrap();
        ProducerWorker {
            fluvio_producer,
            records_to_send: None,
            settings,
            producer_id: format!("producer-{}", generate_random_string(10)),
            tx_to_stats_collector,
        }
    }
    pub async fn prepare_for_batch(&mut self) {
        let records = (0..self.settings.num_records_per_producer_worker_per_batch)
            .map(|i| {
                let key = match self.settings.record_key_allocation_strategy {
                    RecordKeyAllocationStrategy::NoKey => RecordKey::NULL,
                    RecordKeyAllocationStrategy::AllShareSameKey => RecordKey::from(SHARED_KEY),
                    RecordKeyAllocationStrategy::ProducerWorkerUniqueKey => {
                        RecordKey::from(self.producer_id.clone())
                    }
                    RecordKeyAllocationStrategy::RoundRobinKey(x) => {
                        RecordKey::from(format!("rr-{}", i % x))
                    }
                    RecordKeyAllocationStrategy::RandomKey => {
                        RecordKey::from(format!("random-{}", generate_random_string(10)))
                    }
                };
                let data = match self.settings.record_size_strategy {
                    RecordSizeStrategy::Fixed(size) => generate_random_string(size as usize),
                };
                BenchmarkRecord::new(key, data)
            })
            .collect();
        self.records_to_send = Some(records);
    }

    pub async fn send_batch(&mut self) -> Result<(), BenchmarkError> {
        Ok(
            for record in
                self.records_to_send
                    .take()
                    .ok_or(BenchmarkError::ErrorWithExplanation(
                        "prepare_for_batch() not called on PrdoucerWorker".to_string(),
                    ))?
            {
                self.tx_to_stats_collector
                    .send(StatsCollectorMessage::MessageSent(
                        record.hash,
                        Instant::now(),
                    ))
                    .await
                    .map_err(|_| {
                        BenchmarkError::ErrorWithExplanation(
                            "Tx to stats_collector closed".to_string(),
                        )
                    })?;

                self.fluvio_producer
                    .send(record.key, record.data)
                    .await
                    .map_err(BenchmarkError::wrap_err)?;
            },
        )
    }
}
