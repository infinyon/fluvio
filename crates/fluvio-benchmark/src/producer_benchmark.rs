use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use async_channel::{unbounded, Receiver};

use fluvio_future::{task::spawn, future::timeout, timer::sleep};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use crate::{config::ProducerConfig, producer_worker::ProducerWorker, stats_collector::ProduceStat};

pub struct ProducerBenchmark {}

impl ProducerBenchmark {
    pub async fn run_benchmark(config: ProducerConfig) -> Result<()> {
        let topic_name = config.shared_config.topic_config.topic_name.clone();
        let new_topic = TopicSpec::new_computed(
            config.shared_config.topic_config.partitions,
            config.shared_config.topic_config.replicas,
            Some(config.shared_config.topic_config.ignore_rack),
        );
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
        if config.shared_config.topic_config.delete_topic {
            admin.delete::<TopicSpec>(topic_name.clone()).await?;
            print!("Topic deleted successfully {}", topic_name.clone());
        }

        Ok(())
    }

    async fn run_samples(config: ProducerConfig) -> Result<()> {
        let mut tx_controls = Vec::new();
        let mut workers_jh = Vec::new();

        let stat = Arc::new(ProduceStat::default());

        // Set up producers
        for producer_id in 0..config.shared_config.load_config.num_producers {
            println!("starting up producer {}", producer_id);
            let (tx_control, rx_control) = unbounded();
            let worker = ProducerWorker::new(producer_id, config.clone(), stat.clone()).await?;
            let jh = spawn(timeout(
                config.shared_config.worker_timeout,
                ProducerDriver::main_loop(rx_control, worker),
            ));

            tx_control.send(ControlMessage::SendBatch).await?;
            tx_controls.push(tx_control);
            workers_jh.push(jh);
        }
        println!("benchmark started");

        // delay 1 seconds, so produce can start
        sleep(std::time::Duration::from_secs(1)).await;

        let stats = stat.clone();
        spawn(async move {
            loop {
                let stat = stats.clone();
                let run_start_num_messages = stat.message_send.load(Ordering::Relaxed);
                let run_start_bytes = stat.message_bytes.load(Ordering::Relaxed);

                let start_time = std::time::Instant::now();
                sleep(std::time::Duration::from_millis(500)).await;
                let elapse = start_time.elapsed().as_millis();

                let run_end_bytes = stat.message_bytes.load(Ordering::Relaxed);
                let run_end_num_messages = stat.message_send.load(Ordering::Relaxed);
                let bytes_send = run_end_bytes - run_start_bytes;
                let message_send = run_end_num_messages - run_start_num_messages;

                let bytes_per_sec = (bytes_send as f64 / elapse as f64) * 1000.0;
                let human_readable_bytes = format!("{:9.1}mb/s", bytes_per_sec / 1000000.0);
                let message_per_sec = ((message_send as f64 / elapse as f64) * 1000.0).round();
                println!(
                "total bytes send: {} | total message send: {} | message: per second: {}, bytes per  sec: {}, ",
                    bytes_send, message_send, message_per_sec, human_readable_bytes
                );
            }
        });

        // Wait for all producers to finish
        for jh in workers_jh {
            jh.await??;
        }

        // Print stats
        println!("Benchmark completed");
        println!("Stats: {:#?}", stat);

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
