use std::time::Instant;

use anyhow::Result;
use async_channel::{unbounded, Sender, Receiver};
use tracing::{debug, info};

use fluvio_future::{task::spawn, future::timeout, timer::sleep};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use crate::{
    benchmark_config::BenchmarkConfig, producer_worker::ProducerWorker,
    consumer_worker::ConsumerWorker, stats_collector::StatsWorker, stats::AllStatsSync,
};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_samples(config: BenchmarkConfig, all_stats: AllStatsSync) -> Result<()> {
        // Works send results to stats collector
        let (tx_stats, rx_stats) = unbounded();

        // Producers alert driver on success.
        let (tx_success, mut rx_success) = unbounded();
        let mut tx_controls = Vec::new();
        let mut workers_jh = Vec::new();

        // Set up producers
        for producer_id in 0..config.num_concurrent_producer_workers {
            let (tx_control, rx_control) = unbounded();
            let worker = ProducerWorker::new(producer_id, config.clone(), tx_stats.clone()).await?;
            let jh = spawn(timeout(
                config.worker_timeout,
                ProducerDriver::main_loop(rx_control, tx_success.clone(), worker),
            ));
            tx_controls.push(tx_control);
            workers_jh.push(jh);
        }
        debug!("Producer threads spawned successfully");

        // Set up consumers
        // Drivers tell consumers when they can stop trying to consume
        let mut tx_stop = Vec::new();
        for partition in 0..config.num_partitions {
            for consumer_number in 0..config.num_concurrent_consumers_per_partition {
                let (tx_control, rx_control) = unbounded();
                let (tx, rx_stop) = unbounded();
                tx_stop.push(tx);
                let consumer_id = partition * 10000000 + consumer_number;
                let allocation_hint = config.num_records_per_producer_worker_per_batch
                    * config.num_concurrent_producer_workers
                    / config.num_partitions;
                let worker = ConsumerWorker::new(
                    config.clone(),
                    consumer_id,
                    tx_stats.clone(),
                    rx_stop.clone(),
                    partition,
                    allocation_hint,
                )
                .await?;
                let jh = spawn(timeout(
                    config.worker_timeout,
                    ConsumerDriver::main_loop(rx_control, tx_success.clone(), worker),
                ));
                tx_controls.push(tx_control);

                workers_jh.push(jh);
            }
        }
        debug!("Consumer threads spawned successfully");
        let (tx_control, rx_control) = unbounded();
        let worker = StatsWorker::new(tx_stop, rx_stats, config.clone(), all_stats);
        let jh = spawn(timeout(
            config.worker_timeout,
            StatsDriver::main_loop(rx_control, tx_success, worker),
        ));
        workers_jh.push(jh);
        tx_controls.push(tx_control);
        debug!("Stats collector thread spawned successfully");

        let num_expected_messages = workers_jh.len();

        for i in 0..config.num_samples + 1 {
            let now = Instant::now();
            // Prepare for batch
            debug!("Preparing for batch");
            send_control_message(&mut tx_controls, ControlMessage::PrepareForBatch).await?;
            expect_success(&mut rx_success, &config, num_expected_messages).await?;

            // Do the batch
            debug!("Sending batch");
            send_control_message(&mut tx_controls, ControlMessage::SendBatch).await?;
            expect_success(&mut rx_success, &config, num_expected_messages).await?;

            // Clean up the batch
            debug!("Cleaning up batch");
            send_control_message(
                &mut tx_controls,
                ControlMessage::CleanupBatch {
                    produce_stats: i != 0,
                },
            )
            .await?;
            expect_success(&mut rx_success, &config, num_expected_messages).await?;

            // Wait between batches
            debug!(
                "Waiting {:?} between samples",
                config.duration_between_samples
            );

            let elapsed = now.elapsed();
            sleep(config.duration_between_samples).await;

            if i != 0 {
                info!(
                    "Sample {} / {} complete, took {:?} + {:?}",
                    i, config.num_samples, elapsed, config.duration_between_samples
                );
            }
        }
        // Close all worker tasks.
        send_control_message(&mut tx_controls, ControlMessage::Exit).await?;
        for jh in workers_jh {
            timeout(config.worker_timeout, jh).await???;
        }

        Ok(())
    }
    pub async fn run_benchmark(config: BenchmarkConfig, all_stats: AllStatsSync) -> Result<()> {
        // Create topic for this run

        let new_topic = TopicSpec::new_computed(config.num_partitions as u32, 1, None);
        debug!("Create topic spec");
        let admin = FluvioAdmin::connect().await?;
        debug!("Connected to admin");
        admin
            .create(config.topic_name.clone(), false, new_topic)
            .await?;
        debug!("Topic created successfully {}", config.topic_name);
        let result = BenchmarkDriver::run_samples(config.clone(), all_stats.clone()).await;
        // Clean up topic
        admin
            .delete::<TopicSpec, String>(config.topic_name.clone())
            .await?;
        debug!("Topic deleted successfully {}", config.topic_name);

        result?;
        Ok(())
    }
}

async fn send_control_message(
    tx_control: &mut [Sender<ControlMessage>],
    message: ControlMessage,
) -> Result<()> {
    for tx_control in tx_control.iter_mut() {
        tx_control.send(message).await?;
    }
    Ok(())
}

async fn expect_success(
    rx_success: &mut Receiver<Result<()>>,
    config: &BenchmarkConfig,
    num_expected_messages: usize,
) -> Result<()> {
    for _ in 0..num_expected_messages {
        timeout(config.worker_timeout, rx_success.recv()).await???;
    }
    Ok(())
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<()>>,
        mut worker: ProducerWorker,
    ) -> Result<()> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => {
                    worker.prepare_for_batch().await;
                    tx.send(Ok(())).await?;
                }
                ControlMessage::SendBatch => tx.send(worker.send_batch().await).await?,
                ControlMessage::CleanupBatch { .. } => tx.send(Ok(())).await?,
                ControlMessage::Exit => return Ok(()),
            };
        }
    }
}
struct ConsumerDriver;

impl ConsumerDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<()>>,
        mut worker: ConsumerWorker,
    ) -> Result<()> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => tx.send(Ok(())).await?,
                ControlMessage::SendBatch => tx.send(worker.consume().await).await?,
                ControlMessage::CleanupBatch { .. } => tx.send(worker.send_results().await).await?,
                ControlMessage::Exit => return Ok(()),
            };
        }
    }
}

struct StatsDriver;
impl StatsDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<()>>,
        mut worker: StatsWorker,
    ) -> Result<()> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => {
                    tx.send(Ok(())).await?;
                }
                ControlMessage::SendBatch => {
                    tx.send(worker.collect_send_recv_messages().await).await?
                }
                ControlMessage::CleanupBatch { produce_stats } => {
                    let results = worker.validate().await;
                    if produce_stats {
                        worker.compute_stats().await;
                    }
                    worker.new_batch();
                    tx.send(results).await?;
                }
                ControlMessage::Exit => return Ok(()),
            };
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlMessage {
    PrepareForBatch,
    SendBatch,
    CleanupBatch { produce_stats: bool },
    Exit,
}
