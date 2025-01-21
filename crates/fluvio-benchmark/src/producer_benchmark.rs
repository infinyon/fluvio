use anyhow::Result;
use async_channel::{unbounded, Receiver};

use bytesize::ByteSize;
use fluvio_future::{task::spawn, future::timeout, timer::sleep};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use tokio::select;

use crate::{
    config::ProducerConfig, producer_worker::ProducerWorker, stats_collector::StatCollector, utils,
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

        println!("created topic {}", topic_name);
        let result = ProducerBenchmark::run_samples(config.clone()).await;

        sleep(std::time::Duration::from_millis(100)).await;

        if let Err(result_err) = result {
            println!("Error running samples: {:#?}", result_err);
        }

        // Clean up topic
        if config.delete_topic {
            admin.delete::<TopicSpec>(topic_name.clone()).await?;
            print!("Topic deleted successfully {}", topic_name.clone());
        }

        Ok(())
    }

    async fn run_samples(config: ProducerConfig) -> Result<()> {
        let mut tx_controls = Vec::new();
        let mut workers_jh = Vec::new();

        let (stats_sender, stats_receiver) = unbounded();
        let (end_sender, end_receiver) = unbounded();
        let stat_collector =
            StatCollector::create(config.num_records, end_sender.clone(), stats_sender.clone());

        // Set up producers
        for producer_id in 0..config.num_producers {
            let (event_sender, event_receiver) = unbounded();
            stat_collector.add_producer(event_receiver);
            println!("starting up producer {}", producer_id);
            let (tx_control, rx_control) = unbounded();
            let worker = ProducerWorker::new(producer_id, config.clone(), event_sender).await?;
            let jh = spawn(timeout(
                config.worker_timeout,
                ProducerDriver::main_loop(rx_control, worker),
            ));

            tx_control.send(ControlMessage::SendBatch).await?;
            tx_controls.push(tx_control);
            workers_jh.push(jh);
        }
        println!("Benchmark started");

        loop {
            select! {
                stat_rx = stats_receiver.recv() => {
                    if let Ok(stat) = stat_rx {
                        let human_readable_bytes = ByteSize(stat.bytes_per_sec).to_string();
                        println!(
                            //TODO: fix inteval latency
                            //"{} records sent, {} records/sec: ({}/sec), {:.2}ms avg latency, {:.2}ms max latency",
                            "{} records sent, {} records/sec: ({}/sec)",
                             stat.record_send, stat.records_per_sec, human_readable_bytes,
                            //utils::nanos_to_ms_pritable(stat.latency_avg), utils::nanos_to_ms_pritable(stat.latency_max)
                        );
                    }
                }
                end = end_receiver.recv() => {
                    if let Ok(end) = end {
                        let mut latency_yaml = String::new();
                        latency_yaml.push_str(&format!("{:.2}ms avg latency, {:.2}ms max latency",
                            utils::nanos_to_ms_pritable(end.histogram.mean() as u64),
                            utils::nanos_to_ms_pritable(end.histogram.value_at_quantile(1.0))));
                        for percentile in [0.5, 0.95, 0.99] {
                            latency_yaml.push_str(&format!(
                                ", {:.2}ms p{percentile:4.2}",
                                utils::nanos_to_ms_pritable(end.histogram.value_at_quantile(percentile)),
                            ));
                        }
                        println!();
                        println!("{}", latency_yaml);

                        let human_readable_bytes = ByteSize(end.bytes_per_sec).to_string();
                        println!(
                            "{} total records sent, {} records/sec: ({}/sec) ",
                             end.total_records, end.records_per_sec, human_readable_bytes
                        );
                    }
                    break;
                }
            }
        }

        // Wait for all producers to finish
        for jh in workers_jh {
            jh.await??;
        }

        // Print stats
        println!("Benchmark completed");

        Ok(())
    }
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(rx: Receiver<ControlMessage>, worker: ProducerWorker) -> Result<()> {
        //loop {
        match rx.recv().await? {
            ControlMessage::SendBatch => {
                println!("producer send batch");
                if let Err(err) = worker.send_batch().await {
                    println!("producer send batch error: {:#?}", err);
                }
            }
        };
        //}
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlMessage {
    SendBatch,
}
