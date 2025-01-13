use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;

use fluvio::{
    dataplane::{bytes::Bytes, record::RecordData},
    Fluvio, RecordKey, TopicProducerConfigBuilder, TopicProducerPool,
};

use crate::{
    benchmark_config::{
        benchmark_matrix::{RecordKeyAllocationStrategy, SHARED_KEY},
        BenchmarkConfig,
    },
    generate_random_string,
    stats_collector::ProduceStat,
    BenchmarkRecord,
};

pub struct ProducerWorker {
    fluvio_producer: TopicProducerPool,
    records_to_send: Option<Vec<BenchmarkRecord>>,
    config: BenchmarkConfig,
    producer_id: u64,
    stat: Arc<ProduceStat>,
}
impl ProducerWorker {
    pub async fn new(
        producer_id: u64,
        config: BenchmarkConfig,
        stat: Arc<ProduceStat>,
    ) -> Result<Self> {
        let fluvio = Fluvio::connect().await?;

        let fluvio_config = TopicProducerConfigBuilder::default()
            .batch_size(config.producer_batch_size as usize)
            .batch_queue_size(config.producer_queue_size as usize)
            .max_request_size(3200000)
            .linger(config.producer_linger)
            // todo allow alternate partitioner
            .compression(config.producer_compression)
            .timeout(config.producer_server_timeout)
            .isolation(config.producer_isolation)
            .delivery_semantic(config.producer_delivery_semantic)
            .build()?;
        let fluvio_producer = fluvio
            .topic_producer_with_config(config.topic_name.clone(), fluvio_config)
            .await?;
        Ok(ProducerWorker {
            fluvio_producer,
            records_to_send: None,
            config,
            producer_id,
            stat,
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
        println!("producer is sending batch");
        let data_len = self.config.record_size as usize;
        let data = Bytes::from(generate_random_string(data_len));
        //    let record = BenchmarkRecord::new(RecordKey::NULL, rando_datam);
        let record_data: RecordData = data.into();

        loop {
            if self.stat.end.load(Ordering::Relaxed) {
                self.fluvio_producer.flush().await?;
                break;
            }

            //println!("Sending record: {}", records.len());
            self.stat
                .message_bytes
                .fetch_add(data_len as u64, Ordering::Relaxed);
            self.stat.message_send.fetch_add(1, Ordering::Relaxed);

            self.fluvio_producer
                .send(RecordKey::NULL, record_data.clone())
                .await?;
        }

        println!("producer is done sending batch");

        Ok(())
    }
}
