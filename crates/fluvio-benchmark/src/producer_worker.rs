use anyhow::Result;

use fluvio::{
    dataplane::record::RecordData, DeliverySemantic, Fluvio, Isolation, RecordKey,
    TopicProducerConfigBuilder, TopicProducerPool,
};

use crate::{
    config::{ProducerConfig, RecordKeyAllocationStrategy},
    stats_collector::StatCollector,
    utils,
};

const SHARED_KEY: &str = "shared_key";

pub(crate) struct ProducerWorker {
    fluvio_producer: TopicProducerPool,
    records_to_send: Vec<BenchmarkRecord>,
    stat: StatCollector,
}
impl ProducerWorker {
    pub(crate) async fn new(id: u64, config: ProducerConfig, stat: StatCollector) -> Result<Self> {
        let fluvio = Fluvio::connect().await?;

        let fluvio_config = TopicProducerConfigBuilder::default()
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

        let num_records = records_per_producer(id, config.num_producers, config.num_records);

        println!("producer {} will send {} records", id, num_records);

        let records_to_send = create_records(config.clone(), num_records, id);

        println!(
            "producer {} will send {} records",
            id,
            records_to_send.len()
        );

        Ok(ProducerWorker {
            fluvio_producer,
            records_to_send,
            stat,
        })
    }

    pub async fn send_batch(mut self) -> Result<()> {
        println!("producer is sending batch");

        for record in self.records_to_send.into_iter() {
            self.stat.start();
            let time = std::time::Instant::now();
            let send_out = self
                .fluvio_producer
                .send(record.key, record.data.clone())
                .await?;

            self.stat.send_out((send_out, time));
            self.stat.add_record(record.data.len() as u64).await;
        }
        self.fluvio_producer.flush().await?;
        self.stat.finish();

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

/// Calculate the number of records each producer should send
fn records_per_producer(id: u64, num_producers: u64, num_records: u64) -> u64 {
    if id == 0 {
        num_records / num_producers + num_records % num_producers
    } else {
        num_records / num_producers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_num_records_per_producer() {
        let num_producers = 3;
        let num_records = 10;

        assert_eq!(records_per_producer(0, num_producers, num_records), 4);
        assert_eq!(records_per_producer(1, num_producers, num_records), 3);
        assert_eq!(records_per_producer(2, num_producers, num_records), 3);

        let num_producers = 3;
        let num_records = 12;
        assert_eq!(records_per_producer(0, num_producers, num_records), 4);
        assert_eq!(records_per_producer(1, num_producers, num_records), 4);
        assert_eq!(records_per_producer(2, num_producers, num_records), 4);
    }
}
