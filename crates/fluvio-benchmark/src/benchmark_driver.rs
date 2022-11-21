use async_std::{
    channel::{self, Receiver, Sender},
    future::timeout,
};
use fluvio::{metadata::topic::TopicSpec, FluvioAdmin};
use log::{info, debug};

use crate::{
    benchmark_config::benchmark_settings::BenchmarkSettings, producer_worker::ProducerWorker,
    consumer_worker::ConsumerWorker, stats_collector::StatsWorker, BenchmarkError,
};

pub struct BenchmarkDriver {}

impl BenchmarkDriver {
    pub async fn run_sample(settings: BenchmarkSettings) -> Result<(), BenchmarkError> {
        // Works send results to stats collector
        let (tx_stats, rx_stats) = channel::unbounded();

        // Producers alert driver on success.
        let (tx_success, mut rx_success) = channel::unbounded();
        let mut tx_controls = Vec::new();
        let mut workers_jh = Vec::new();

        // Set up producers
        for producer_id in 0..settings.num_concurrent_producer_workers {
            let (tx_control, rx_control) = channel::unbounded();
            let worker =
                ProducerWorker::new(producer_id, settings.clone(), tx_stats.clone()).await?;
            let jh = async_std::task::spawn(timeout(
                settings.worker_timeout,
                ProducerDriver::main_loop(rx_control, tx_success.clone(), worker),
            ));
            tx_controls.push(tx_control);
            workers_jh.push(jh);
        }
        debug!("Producer threads spawned successfully");

        // Set up consumers
        // Drivers tell consumers when they can stop trying to consume
        let (tx_stop, rx_stop) = channel::unbounded();
        for partition in 0..settings.num_partitions {
            for consumer_number in 0..settings.num_concurrent_consumers_per_partition {
                let (tx_control, rx_control) = channel::unbounded();
                let consumer_id = partition * 10000000 + consumer_number;
                let allocation_hint = settings.num_records_per_producer_worker_per_batch
                    * settings.num_concurrent_producer_workers
                    / settings.num_partitions;
                let worker = ConsumerWorker::new(
                    settings.clone(),
                    consumer_id,
                    tx_stats.clone(),
                    rx_stop.clone(),
                    partition,
                    allocation_hint,
                )
                .await?;
                let jh = async_std::task::spawn(timeout(
                    settings.worker_timeout,
                    ConsumerDriver::main_loop(rx_control, tx_success.clone(), worker),
                ));
                tx_controls.push(tx_control);

                workers_jh.push(jh);
            }
        }
        debug!("Consumer threads spawned successfully");
        let (tx_control, rx_control) = channel::unbounded();
        let worker = StatsWorker::new(tx_stop, rx_stats, settings.clone());
        let jh = async_std::task::spawn(timeout(
            settings.worker_timeout,
            StatsDriver::main_loop(rx_control, tx_success, worker),
        ));
        workers_jh.push(jh);
        tx_controls.push(tx_control);
        debug!("Stats collector thread spawned successfully");

        let num_expected_messages = workers_jh.len();
        for i in 0..settings.num_batches_per_sample {
            debug!(
                "Starting batch {} of {}",
                i, settings.num_batches_per_sample
            );
            // Prepare for batch
            debug!("Preparing for batch");
            send_control_message(&mut tx_controls, ControlMessage::PrepareForBatch).await?;
            expect_success(&mut rx_success, &settings, num_expected_messages).await?;

            // Do the batch
            debug!("Sending batch");
            send_control_message(&mut tx_controls, ControlMessage::SendBatch).await?;
            expect_success(&mut rx_success, &settings, num_expected_messages).await?;

            // Clean up the batch
            debug!("Cleaning up batch");
            send_control_message(&mut tx_controls, ControlMessage::CleanupBatch).await?;
            expect_success(&mut rx_success, &settings, num_expected_messages).await?;
        }
        // Close all worker tasks.
        send_control_message(&mut tx_controls, ControlMessage::Exit).await?;
        for jh in workers_jh {
            timeout(settings.worker_timeout, jh).await???;
        }

        Ok(())
    }
    pub async fn run_benchmark(settings: BenchmarkSettings) -> Result<(), BenchmarkError> {
        for i in 0..settings.num_samples {
            info!("Beginning sample {i}");
            // Create topic for this run
            let new_topic = TopicSpec::new_computed(settings.num_partitions as u32, 1, None);
            let admin = FluvioAdmin::connect().await?;
            admin
                .create(settings.topic_name.clone(), false, new_topic)
                .await?;
            debug!("Topic created successfully {}", settings.topic_name);
            let result = BenchmarkDriver::run_sample(settings.clone()).await;
            // Clean up topic
            let _ = admin
                .delete::<TopicSpec, String>(settings.topic_name.clone())
                .await?;
            debug!("Topic deleted successfully {}", settings.topic_name);

            result?;
        }
        Ok(())
    }
}

async fn send_control_message(
    tx_control: &mut Vec<Sender<ControlMessage>>,
    message: ControlMessage,
) -> Result<(), BenchmarkError> {
    for tx_control in tx_control.iter_mut() {
        tx_control.send(message).await?;
    }
    Ok(())
}

async fn expect_success(
    rx_success: &mut Receiver<Result<(), BenchmarkError>>,
    settings: &BenchmarkSettings,
    num_expected_messages: usize,
) -> Result<(), BenchmarkError> {
    for _ in 0..num_expected_messages {
        timeout(settings.worker_timeout, rx_success.recv()).await???;
    }
    Ok(())
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<(), BenchmarkError>>,
        mut worker: ProducerWorker,
    ) -> Result<(), BenchmarkError> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => {
                    worker.prepare_for_batch().await;
                    tx.send(Ok(())).await?;
                }
                ControlMessage::SendBatch => tx.send(worker.send_batch().await).await?,
                ControlMessage::CleanupBatch => tx.send(Ok(())).await?,
                ControlMessage::Exit => return Ok(()),
            };
        }
    }
}
struct ConsumerDriver;

impl ConsumerDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<(), BenchmarkError>>,
        mut worker: ConsumerWorker,
    ) -> Result<(), BenchmarkError> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => tx.send(Ok(())).await?,
                ControlMessage::SendBatch => tx.send(worker.consume().await).await?,
                ControlMessage::CleanupBatch => tx.send(worker.send_results().await).await?,
                ControlMessage::Exit => return Ok(()),
            };
        }
    }
}

struct StatsDriver;
impl StatsDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<(), BenchmarkError>>,
        mut worker: StatsWorker,
    ) -> Result<(), BenchmarkError> {
        loop {
            match rx.recv().await? {
                ControlMessage::PrepareForBatch => {
                    tx.send(Ok(())).await?;
                }
                ControlMessage::SendBatch => {
                    tx.send(worker.collect_send_recv_messages().await).await?
                }
                ControlMessage::CleanupBatch => {
                    let results = worker.validate().await;
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
    CleanupBatch,
    Exit,
}
