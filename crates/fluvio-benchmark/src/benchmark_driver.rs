use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use async_channel::{unbounded, Receiver};
use tracing::debug;

use fluvio_future::{task::spawn, future::timeout, timer::sleep};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use crate::{
    benchmark_config::BenchmarkConfig, producer_worker::ProducerWorker,
    stats_collector::ProduceStat,
};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_samples(config: BenchmarkConfig) -> Result<()> {
        // Works send results to stats collector
        //let (tx_stats, rx_stats) = unbounded();

        let mut tx_controls = Vec::new();
        let mut workers_jh = Vec::new();

        let stat = Arc::new(ProduceStat::default());

        // Set up producers
        for producer_id in 0..config.num_concurrent_producer_workers {
            println!("starting up producer {}", producer_id);
            let (tx_control, rx_control) = unbounded();
            let worker = ProducerWorker::new(producer_id, config.clone(), stat.clone()).await?;
            let jh = spawn(timeout(
                config.worker_timeout,
                ProducerDriver::main_loop(rx_control, worker),
            ));

            tx_control.send(ControlMessage::SendBatch).await?;
            tx_controls.push(tx_control);
            workers_jh.push(jh);
        }
        println!("benchmark started");

        // sleep every second
        let max_run = 20;

        // delay 1 seconds, so produce can start
        sleep(std::time::Duration::from_secs(1)).await;

        for _run in 0..max_run {
            // get current stats

            let run_start_num_messages = stat.message_send.load(Ordering::Relaxed);
            let run_start_bytes = stat.message_bytes.load(Ordering::Relaxed);

            let start_time = std::time::Instant::now();
            sleep(std::time::Duration::from_secs(2)).await;
            let elapse = start_time.elapsed().as_millis();

            let run_end_bytes = stat.message_bytes.load(Ordering::Relaxed);
            let run_end_num_messages = stat.message_send.load(Ordering::Relaxed);
            let bytes_send = run_end_bytes - run_start_bytes;
            let message_send = run_end_num_messages - run_start_num_messages;

            println!("total bytes send: {}", bytes_send);
            println!("total message send: {}", message_send);
            let bytes_per_sec = (bytes_send as f64 / elapse as f64) * 1000.0;
            let human_readable_bytes = format!("{:9.1}mb/s", bytes_per_sec / 1000000.0);
            let message_per_sec = ((message_send as f64 / elapse as f64) * 1000.0).round();
            println!(
                "message: per second: {}, bytes per  sec: {}, ",
                message_per_sec, human_readable_bytes
            );
        }

        stat.end.store(true, Ordering::Relaxed);

        Ok(())
    }
    pub async fn run_benchmark(config: BenchmarkConfig) -> Result<()> {
        // Create topic for this run

        let new_topic = TopicSpec::new_computed(config.num_partitions as u32, 1, None);
        let admin = FluvioAdmin::connect().await?;
        debug!("Connected to admin");
        admin
            .create(config.topic_name.clone(), false, new_topic)
            .await?;
        println!("created topic {}", config.topic_name);
        let result = BenchmarkDriver::run_samples(config.clone()).await;

        println!("Benchmark completed");
        sleep(std::time::Duration::from_millis(100)).await;

        if let Err(result_err) = result {
            println!("Error running samples: {:#?}", result_err);
        }
        // Clean up topic
        admin.delete::<TopicSpec>(config.topic_name.clone()).await?;
        print!("Topic deleted successfully {}", config.topic_name);

        Ok(())
    }
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(rx: Receiver<ControlMessage>, mut worker: ProducerWorker) -> Result<()> {
        loop {
            match rx.recv().await? {
                ControlMessage::SendBatch => {
                    println!("producer send batch");
                    if let Err(err) = worker.send_batch().await {
                        println!("producer send batch error: {:#?}", err);
                    }
                }
            };
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlMessage {
    SendBatch,
}
