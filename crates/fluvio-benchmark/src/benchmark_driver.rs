use async_std::channel;
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};

use crate::{
    benchmark_config::benchmark_settings::BenchmarkSettings, producer_worker::ProducerWorker,
    consumer_worker::ConsumerWorker, stats_collector::StatsCollector,
};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_sample(settings: BenchmarkSettings) {
        // Create topic for this run
        let new_topic = TopicSpec::new_computed(settings.num_partitions as u32, 1, None);
        let admin = FluvioAdmin::connect().await.unwrap();
        admin
            .create(settings.topic_name.clone(), false, new_topic)
            .await
            .unwrap();

        let (tx_stats, rx_stats) = channel::unbounded();
        let mut producers = Vec::new();
        for producer_id in 0..settings.num_concurrent_producer_workers {
            let worker = ProducerWorker::new(producer_id, settings.clone(), tx_stats.clone()).await;
            producers.push(worker);
        }
        let mut consumers = Vec::new();
        for partition in 0..settings.num_partitions {
            for consumer_number in 0..settings.num_concurrent_consumers_per_partition {
                let consumer_id = partition * 10000000 + consumer_number;
                let allocation_hint = settings.num_records_per_producer_worker_per_batch
                    * settings.num_concurrent_producer_workers
                    / settings.num_partitions;
                let worker = ConsumerWorker::new(
                    settings.clone(),
                    consumer_id,
                    tx_stats.clone(),
                    partition,
                    allocation_hint,
                )
                .await;

                consumers.push(worker);
            }
        }
        let stats_collector = StatsCollector::new(rx_stats, settings.clone());

        for _ in 0..settings.num_batches_per_sample {
            // prepare producers
            for pw in producers.iter_mut() {}
        }

        // Clean up topic
        let _ = admin.delete::<TopicSpec, String>(settings.topic_name).await;
    }
    pub async fn run_benchmark(settings: BenchmarkSettings) {
        for _ in 0..settings.num_samples {
            BenchmarkDriver::run_sample(settings.clone()).await;
        }
    }
}
