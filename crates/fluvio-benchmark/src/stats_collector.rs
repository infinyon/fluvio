use std::{
    f64,
    sync::{atomic::AtomicU64, Arc, OnceLock},
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use fluvio::ProduceCompletionBatchEvent;
use fluvio_future::{sync::RwLock, task::spawn, timer::sleep};
use hdrhistogram::Histogram;
use tracing::trace;

pub(crate) struct ProducerStat {}

pub struct TotalStats {
    record_send: AtomicU64,
    record_bytes: AtomicU64,
    first_start_time: OnceLock<Instant>,
}

pub struct Stats {
    pub record_send: u64,
    pub record_bytes: u64,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub latency_avg: u64,
    pub latency_max: u64,
}

pub struct EndProducerStat {
    pub latencies_histogram: Histogram<u64>,
    pub total_records: u64,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub elapsed: Duration,
}

impl ProducerStat {
    pub(crate) fn new(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        total_stats: Arc<TotalStats>,
        latencies: Arc<RwLock<Vec<u64>>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
    ) -> Self {
        Self::track_producer_stats(
            num_records,
            end_sender,
            latencies.clone(),
            event_receiver,
            total_stats.clone(),
        );

        Self {}
    }

    fn track_producer_stats(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        latencies: Arc<RwLock<Vec<u64>>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
        total_stats: Arc<TotalStats>,
    ) {
        spawn(async move {
            let latency_histogram = latencies.clone();
            while let Ok(event) = event_receiver.recv().await {
                let latencies = latency_histogram.clone();
                let total_stats = total_stats.clone();
                let end_sender = end_sender.clone();
                total_stats
                    .first_start_time
                    .get_or_init(|| event.created_at);
                spawn(async move {
                    let mut lantencies = latencies.write().await;
                    lantencies.push(event.elapsed.as_nanos() as u64);
                    drop(lantencies);

                    total_stats
                        .record_send
                        .fetch_add(event.records_len, std::sync::atomic::Ordering::Relaxed);
                    total_stats
                        .record_bytes
                        .fetch_add(event.bytes_size, std::sync::atomic::Ordering::Relaxed);

                    ProducerStat::send_end(num_records, end_sender, latencies, total_stats).await;
                });
            }
        });
    }

    async fn send_end(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
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
            if let Err(e) = end_sender.send(end).await {
                trace!("error sending end: {}", e);
            }
        }
    }
}

pub(crate) struct StatCollector {
    num_records: u64,
    total_stats: Arc<TotalStats>,
    end_sender: Sender<EndProducerStat>,
    latencies_histogram: Arc<RwLock<Vec<u64>>>,
}

impl StatCollector {
    pub(crate) fn create(
        num_records: u64,
        stats_sender: Sender<Stats>,
        end_sender: Sender<EndProducerStat>,
    ) -> Self {
        let latencies = Arc::new(RwLock::new(Vec::with_capacity(num_records as usize)));

        let total_stats = Arc::new(TotalStats {
            record_send: AtomicU64::new(0),
            record_bytes: AtomicU64::new(0),
            first_start_time: OnceLock::new(),
        });

        Self::send_stats(
            num_records,
            latencies.clone(),
            stats_sender.clone(),
            total_stats.clone(),
        );

        Self {
            total_stats,
            num_records,
            end_sender,
            latencies_histogram: latencies,
        }
    }

    pub(crate) fn add_producer(&self, event_receiver: Receiver<ProduceCompletionBatchEvent>) {
        ProducerStat::new(
            self.num_records,
            self.end_sender.clone(),
            self.total_stats.clone(),
            self.latencies_histogram.clone(),
            event_receiver,
        );
    }

    fn send_stats(
        num_records: u64,
        latencies: Arc<RwLock<Vec<u64>>>,
        stats_sender: Sender<Stats>,
        total_stats: Arc<TotalStats>,
    ) {
        spawn(async move {
            loop {
                let stats_sender = stats_sender.clone();
                let total_stats = Arc::clone(&total_stats);
                let old_record_send = total_stats
                    .record_send
                    .load(std::sync::atomic::Ordering::Relaxed);
                if old_record_send >= num_records {
                    break;
                }

                let old_record_bytes = total_stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                let old_latencies = latencies.read().await;
                let old_latencies_len = old_latencies.len();
                drop(old_latencies);
                sleep(Duration::from_secs(1)).await;
                let first_start_time = total_stats.first_start_time.get();
                if first_start_time.is_none() {
                    continue;
                }

                let total_record_send = total_stats
                    .record_send
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total_record_bytes = total_stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);

                if total_record_send == old_record_send {
                    continue;
                }

                let record_send = total_record_send - old_record_send;
                let record_bytes = total_record_bytes - old_record_bytes;
                let elapsed = first_start_time.expect("start time").elapsed();
                let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;
                let records_per_sec = (total_record_send as f64 / elapsed_seconds).round() as u64;
                let bytes_per_sec = (total_record_bytes as f64 / elapsed_seconds).round() as u64;

                let total_latencies = latencies.read().await;

                let new_latencies = total_latencies
                    .clone()
                    .into_iter()
                    .skip(old_latencies_len)
                    .collect::<Vec<_>>();
                drop(total_latencies);

                let mut latencies_histogram =
                    hdrhistogram::Histogram::<u64>::new(3).expect("new histogram");
                for value in new_latencies {
                    latencies_histogram.record(value).expect("record");
                }

                let latency_avg = latencies_histogram.mean() as u64;
                let latency_max = latencies_histogram.value_at_quantile(1.0);

                // ignore if channel is closed
                if let Err(e) = stats_sender
                    .send(Stats {
                        record_send,
                        record_bytes,
                        records_per_sec,
                        bytes_per_sec,
                        latency_avg,
                        latency_max,
                    })
                    .await
                {
                    trace!("error sending stats: {}", e);
                }
            }
        });
    }
}
