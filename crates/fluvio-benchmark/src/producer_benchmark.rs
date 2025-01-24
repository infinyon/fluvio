use std::sync::Arc;

use anyhow::Result;
use async_channel::unbounded;

use bytesize::ByteSize;
use fluvio_future::{future::timeout, task::spawn, timer::sleep};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::sync::broadcast;
use tracing::debug;

use crate::{
    config::ProducerConfig,
    producer_worker::ProducerWorker,
    stats_collector::{EndProducerStat, StatCollector, Stats},
    utils,
};

pub struct ProducerBenchmark {}

impl ProducerBenchmark {
    pub async fn run_benchmark(config: ProducerConfig) -> Result<()> {
        let topic_name = config.topic_name.clone();
        let new_topic =
            TopicSpec::new_computed(config.partitions, config.replicas, Some(config.ignore_rack));
        let admin = FluvioAdmin::connect().await?;

        // Create topic if it doesn't exist
        if admin
            .list::<TopicSpec, String>([topic_name.clone()].to_vec())
            .await?
            .is_empty()
        {
            admin.create(topic_name.clone(), false, new_topic).await?;
        }

        debug!("created topic {}", topic_name);
        let result = ProducerBenchmark::run_samples(config.clone()).await;

        sleep(std::time::Duration::from_millis(100)).await;

        if let Err(result_err) = result {
            println!("Error running samples: {:#?}", result_err);
        }

        // Clean up topic
        if config.delete_topic {
            admin.delete::<TopicSpec>(topic_name.clone()).await?;
            debug!("Topic deleted successfully {}", topic_name.clone());
        }

        Ok(())
    }

    async fn run_samples(config: ProducerConfig) -> Result<()> {
        let (stats_sender, stats_receiver) = unbounded();
        let (end_sender, mut end_receiver) = broadcast::channel(2);
        let end_sender = Arc::new(end_sender);
        let stat_collector =
            StatCollector::create(config.num_records, stats_sender.clone(), end_sender.clone());

        Self::setup_producers(config.clone(), stat_collector).await;
        println!("Benchmark started");
        Self::print_progress_on_backgroud(stats_receiver).await;
        Self::print_benchmark_on_end(&mut end_receiver).await;
        println!("Benchmark completed");

        Ok(())
    }

    async fn setup_producers(config: ProducerConfig, stat_collector: StatCollector) {
        spawn(async move {
            let worker_futures = FuturesUnordered::new();
            for producer_id in 0..config.num_producers {
                let (event_sender, event_receiver) = unbounded();
                stat_collector.add_producer(event_receiver);
                let config = config.clone();
                let jh = spawn(timeout(config.worker_timeout, async move {
                    debug!("starting up producer {}", producer_id);
                    let worker = ProducerWorker::new(producer_id, config, event_sender)
                        .await
                        .expect("create producer worker");
                    ProducerDriver::main_loop(worker).await.expect("main loop");
                }));

                worker_futures.push(jh);
            }

            for worker in worker_futures.collect::<Vec<_>>().await {
                worker.expect("producer worker failed");
            }
        });
    }

    async fn print_progress_on_backgroud(stats_receiver: async_channel::Receiver<Stats>) {
        spawn(async move {
            while let Ok(stat) = stats_receiver.recv().await {
                let human_readable_bytes = ByteSize(stat.bytes_per_sec).to_string();
                println!(
                    "{} records sent, {} records/sec: ({}/sec), {} avg latency, {} max latency",
                    stat.record_send,
                    stat.records_per_sec,
                    human_readable_bytes,
                    utils::nanos_to_ms_pritable(stat.latency_avg),
                    utils::nanos_to_ms_pritable(stat.latency_max)
                );
            }
        });
    }

    async fn print_benchmark_on_end(end_receiver: &mut broadcast::Receiver<EndProducerStat>) {
        if let Ok(end) = end_receiver.recv().await {
            // sleep enough time to make sure all stats are printed
            sleep(std::time::Duration::from_secs(1)).await;
            let mut latency_yaml = String::new();
            latency_yaml.push_str(&format!(
                "{} avg latency, {} max latency",
                utils::nanos_to_ms_pritable(end.latencies_histogram.mean() as u64),
                utils::nanos_to_ms_pritable(end.latencies_histogram.value_at_quantile(1.0))
            ));
            for percentile in [0.5, 0.95, 0.99] {
                latency_yaml.push_str(&format!(
                    ", {} p{percentile:4.2}",
                    utils::nanos_to_ms_pritable(
                        end.latencies_histogram.value_at_quantile(percentile)
                    ),
                ));
            }
            println!();
            println!("{}", latency_yaml);

            let human_readable_bytes = ByteSize(end.bytes_per_sec).to_string();
            println!(
                "{} total records sent, {} records/sec: ({}/sec), total time: {}",
                end.total_records,
                end.records_per_sec,
                human_readable_bytes,
                utils::pretty_duration(end.elapsed)
            );
        }
    }
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(worker: ProducerWorker) -> Result<()> {
        worker.send_batch().await?;
        Ok(())
    }
}
