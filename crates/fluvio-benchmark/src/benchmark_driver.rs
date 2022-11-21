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
            let worker = ProducerWorker::new(producer_id, settings.clone(), tx_stats.clone()).await;
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
                .await;
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

        Ok(())
    }
    pub async fn run_benchmark(settings: BenchmarkSettings) -> Result<(), BenchmarkError> {
        for i in 0..settings.num_samples {
            info!("Beginning sample {i}");
            // Create topic for this run
            let new_topic = TopicSpec::new_computed(settings.num_partitions as u32, 1, None);
            let admin = FluvioAdmin::connect().await.unwrap();
            admin
                .create(settings.topic_name.clone(), false, new_topic)
                .await
                .or_else(|e| {
                    Err(BenchmarkError::ErrorWithExplanation(format!(
                        "Failed to create topic: {:?}",
                        e
                    )))
                })?;
            debug!("Topic created successfully {}", settings.topic_name);
            let result = BenchmarkDriver::run_sample(settings.clone()).await;
            // Clean up topic
            let _ = admin
                .delete::<TopicSpec, String>(settings.topic_name.clone())
                .await
                .or_else(|_| {
                    Err(BenchmarkError::ErrorWithExplanation(
                        "Failed to delete topic".to_string(),
                    ))
                })?;
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
        tx_control.send(message).await.or_else(|_| {
            Err(BenchmarkError::ErrorWithExplanation(
                "Failed to send control message".to_string(),
            ))
        })?;
    }
    Ok(())
}

async fn expect_success(
    rx_success: &mut Receiver<Result<(), BenchmarkError>>,
    settings: &BenchmarkSettings,
    num_expected_messages: usize,
) -> Result<(), BenchmarkError> {
    for _ in 0..num_expected_messages {
        timeout(settings.worker_timeout, rx_success.recv())
            .await
            .or_else(|_| Err(BenchmarkError::Timeout))?
            .or_else(|_| {
                Err(BenchmarkError::ErrorWithExplanation(
                    "Failed to recv".to_string(),
                ))
            })??;
        info!("Received success message");
    }
    Ok(())
}

struct ProducerDriver;

impl ProducerDriver {
    async fn main_loop(
        rx: Receiver<ControlMessage>,
        tx: Sender<Result<(), BenchmarkError>>,
        mut worker: ProducerWorker,
    ) {
        loop {
            match rx.recv().await {
                Ok(control_message) => match control_message {
                    ControlMessage::PrepareForBatch => {
                        worker.prepare_for_batch().await;
                        tx.send(Ok(())).await.unwrap();
                        debug!("Producer sent success message");
                    }
                    ControlMessage::SendBatch => tx.send(worker.send_batch().await).await.unwrap(),
                    ControlMessage::CleanupBatch => tx.send(Ok(())).await.unwrap(),
                },
                Err(_) => return,
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
    ) {
        loop {
            match rx.recv().await {
                Ok(control_message) => match control_message {
                    ControlMessage::PrepareForBatch => {
                        tx.send(Ok(())).await.unwrap();
                        debug!("Consumer sent success message");
                    }
                    ControlMessage::SendBatch => tx.send(worker.consume().await).await.unwrap(),

                    ControlMessage::CleanupBatch => {
                        worker.send_results().await;
                        tx.send(Ok(())).await.unwrap()
                    }
                },
                Err(_) => return,
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
    ) {
        loop {
            match rx.recv().await {
                Ok(control_message) => match control_message {
                    ControlMessage::PrepareForBatch => {
                        tx.send(Ok(())).await.unwrap();
                        debug!("Stats driver sent success message");
                    }
                    ControlMessage::SendBatch => tx
                        .send(worker.collect_send_recv_messages().await)
                        .await
                        .unwrap(),
                    ControlMessage::CleanupBatch => {
                        let results = worker.validate().await;
                        worker.new_batch();
                        tx.send(results).await.unwrap()
                    }
                },
                Err(_) => return,
            };
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ControlMessage {
    PrepareForBatch,
    SendBatch,
    CleanupBatch,
}
