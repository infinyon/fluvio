use std::collections::VecDeque;

use fluvio::{TopicProducer, RecordKey, Fluvio, TopicProducerConfigBuilder};

use crate::{
    benchmark_config::{
        benchmark_settings::BenchmarkSettings,
        benchmark_matrix::{RecordKeyAllocationStrategy, RecordSizeStrategy},
    },
    BenchmarkRecord, generate_random_string, BenchmarkError, SHARED_KEY,
};

#[deprecated]
pub struct Producer {
    pub producer: TopicProducer,
    pub data: VecDeque<String>,
    pub records_per_batch: usize,
}

impl Producer {
    pub async fn produce(mut self) {
        for _ in 0..self.records_per_batch {
            self.producer
                .send(RecordKey::NULL, self.data.pop_front().unwrap())
                .await
                .unwrap();
            self.producer.flush().await.unwrap();
        }
        // self.producer.flush().await.unwrap();
    }
}

pub struct ProducerWorker {
    fluvio_producer: TopicProducer,
    records_to_send: Option<Vec<BenchmarkRecord>>,
    settings: BenchmarkSettings,
    producer_id: String,
}
impl ProducerWorker {
    pub async fn new(settings: BenchmarkSettings) -> Self {
        let fluvio = Fluvio::connect().await.unwrap();

        let config = TopicProducerConfigBuilder::default()
            .batch_size(settings.producer_batch_size)
            .batch_queue_size(settings.producer_queue_size)
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
                    RecordSizeStrategy::Fixed(size) => generate_random_string(size),
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
                        "prepare_for_batch() not called on PrdoucerWorker",
                    ))?
            {
                // TODO notify stats / controller

                self.fluvio_producer
                    .send(record.key, record.data)
                    .await
                    .map_err(BenchmarkError::wrap_err)?;
            },
        )
    }
}
