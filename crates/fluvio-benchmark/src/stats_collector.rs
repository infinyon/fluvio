use std::{
    f64,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use async_lock::RwLock;
use fluvio::ProduceCompletionBatchEvent;
use fluvio_future::task::spawn;
use hdrhistogram::Histogram;
use once_cell::sync::OnceCell;
use tokio::{select, sync::broadcast};
use tracing::trace;

pub(crate) struct ProducerStat {}

pub struct TotalStats {
    record_send: AtomicU64,
    record_bytes: AtomicU64,
    first_start_time: OnceCell<Instant>,
}

pub struct CentralStats {
    pub record_send: u64,
    pub record_bytes: u64,
    pub latency: u64,
}

pub struct Stats {
    pub record_send: u64,
    pub record_bytes: u64,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub latency_avg: u64,
    pub latency_max: u64,
}

#[derive(Clone)]
pub struct EndProducerStat {
    pub latencies_histogram: Histogram<u64>,
    pub total_records: u64,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub elapsed: Duration,
}

impl ProducerStat {
    pub(crate) fn new(
        central_stats_tx: Sender<CentralStats>,
        num_records: u64,
        end_sender: Arc<broadcast::Sender<EndProducerStat>>,
        total_stats: Arc<TotalStats>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
    ) -> Self {
        Self::track_producer_stats(
            central_stats_tx,
            num_records,
            end_sender,
            event_receiver,
            total_stats.clone(),
        );

        Self {}
    }

    fn track_producer_stats(
        central_stats_tx: Sender<CentralStats>,
        num_records: u64,
        end_sender: Arc<broadcast::Sender<EndProducerStat>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
        total_stats: Arc<TotalStats>,
    ) {
        let latencies = Arc::new(RwLock::new(Vec::with_capacity(num_records as usize)));
        spawn(async move {
            while let Ok(event) = event_receiver.recv().await {
                total_stats
                    .first_start_time
                    .get_or_init(|| event.created_at);
                let latencies = latencies.clone();
                let total_stats = total_stats.clone();
                let end_sender = end_sender.clone();
                let central_stats_tx = central_stats_tx.clone();
                spawn(async move {
                    let mut write_latencies = latencies.write().await;
                    write_latencies.push(event.elapsed.as_nanos() as u64);
                    drop(write_latencies);

                    total_stats
                        .record_send
                        .fetch_add(event.records_len, std::sync::atomic::Ordering::Relaxed);
                    total_stats
                        .record_bytes
                        .fetch_add(event.bytes_size, std::sync::atomic::Ordering::Relaxed);

                    central_stats_tx
                        .send(CentralStats {
                            record_send: event.records_len,
                            record_bytes: event.bytes_size,
                            latency: event.elapsed.as_nanos() as u64,
                        })
                        .await
                        .expect("send stats");

                    ProducerStat::send_end(num_records, end_sender, latencies, total_stats).await;
                });
            }
        });
    }

    async fn send_end(
        num_records: u64,
        end_sender: Arc<broadcast::Sender<EndProducerStat>>,
        latencies: Arc<RwLock<Vec<u64>>>,
        total_stats: Arc<TotalStats>,
    ) {
        let latency_histogram = latencies.clone();

        let record_send = total_stats
            .record_send
            .load(std::sync::atomic::Ordering::Relaxed);

        if record_send >= num_records {
            let record_bytes = total_stats
                .record_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let latency_histogram = latency_histogram.read().await;
            let elapsed = total_stats
                .first_start_time
                .get()
                .expect("start time")
                .elapsed();

            let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;
            let records_per_sec = (record_send as f64 / elapsed_seconds).round() as u64;
            let bytes_per_sec = (record_bytes as f64 / elapsed_seconds).round() as u64;

            let mut latencies_histogram = Histogram::<u64>::new(3).expect("new histogram");
            for value in latency_histogram.iter() {
                latencies_histogram.record(*value).expect("record");
            }

            let end = EndProducerStat {
                latencies_histogram,
                total_records: record_send,
                records_per_sec,
                bytes_per_sec,
                elapsed,
            };

            // check if any producer already sent it
            if let Err(e) = end_sender.send(end) {
                trace!("error sending end: {}", e);
            }
        }
    }
}

pub(crate) struct StatCollector {
    num_records: u64,
    total_stats: Arc<TotalStats>,
    end_sender: Arc<broadcast::Sender<EndProducerStat>>,
    central_stats_tx: Sender<CentralStats>,
}

impl StatCollector {
    pub(crate) fn create(
        num_records: u64,
        print_stats_sender: Sender<Stats>,
        end_sender: Arc<broadcast::Sender<EndProducerStat>>,
    ) -> Self {
        let (central_stats_tx, central_stats_rx) = async_channel::unbounded();

        let total_stats = Arc::new(TotalStats {
            record_send: AtomicU64::new(0),
            record_bytes: AtomicU64::new(0),
            first_start_time: OnceCell::new(),
        });

        Self::send_central_stats(
            print_stats_sender.clone(),
            end_sender.clone(),
            central_stats_rx,
        );

        Self {
            total_stats,
            num_records,
            end_sender,
            central_stats_tx,
        }
    }

    pub(crate) fn add_producer(&self, event_receiver: Receiver<ProduceCompletionBatchEvent>) {
        ProducerStat::new(
            self.central_stats_tx.clone(),
            self.num_records,
            self.end_sender.clone(),
            self.total_stats.clone(),
            event_receiver,
        );
    }

    fn send_central_stats(
        stats_sender: Sender<Stats>,
        end_broadcast: Arc<broadcast::Sender<EndProducerStat>>,
        central_stats_rx: Receiver<CentralStats>,
    ) {
        spawn(async move {
            let mut is_the_first = true;
            let mut instant = Instant::now();
            let mut central_stats = CentralStats {
                record_send: 0,
                record_bytes: 0,
                latency: 0,
            };
            let mut latencies_histogram =
                hdrhistogram::Histogram::<u64>::new(3).expect("new histogram");

            let mut end_broadcast = end_broadcast.subscribe();

            loop {
                select! {
                    stat = central_stats_rx.recv() => {
                        if let Ok(stats) = stat {
                            if is_the_first {
                                is_the_first = false;
                                instant = Instant::now();
                            }

                            let elapsed = instant.elapsed();
                            let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;

                            central_stats.record_send += stats.record_send;
                            central_stats.record_bytes += stats.record_bytes;
                            central_stats.latency += stats.latency;
                            latencies_histogram.record(stats.latency).expect("record");

                            if elapsed.as_secs() >= 5 {
                                let latency_avg = latencies_histogram.mean() as u64;
                                let latency_max = latencies_histogram.value_at_quantile(1.0);
                                let records_per_sec =
                                    (central_stats.record_send as f64 / elapsed_seconds).round() as u64;
                                let bytes_per_sec =
                                    (central_stats.record_bytes as f64 / elapsed_seconds).round() as u64;

                                let _ = stats_sender
                                    .send(Stats {
                                        record_send: central_stats.record_send,
                                        record_bytes: central_stats.record_bytes,
                                        records_per_sec,
                                        bytes_per_sec,
                                        latency_avg,
                                        latency_max,
                                    })
                                    .await;

                                instant = Instant::now();
                                central_stats = CentralStats {
                                    record_send: 0,
                                    record_bytes: 0,
                                    latency: 0,
                                };
                                latencies_histogram =
                                    hdrhistogram::Histogram::<u64>::new(3).expect("new histogram");
                            }
                        }
                    }
                    _ = end_broadcast.recv() => {
                        break
                    }
                }
            }
        });
    }
}
