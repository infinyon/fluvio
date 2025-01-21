use std::{
    f64,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use fluvio::ProduceCompletionBatchEvent;
use fluvio_future::{
    sync::{Mutex, RwLock},
    task::spawn,
    timer::sleep,
};
use hdrhistogram::Histogram;

pub(crate) struct ProducerStat {}

pub struct TotalStats {
    record_send: AtomicU64,
    record_bytes: AtomicU64,
    first_start_time: RwLock<Option<Instant>>,
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
    pub histogram: Histogram<u64>,
    pub total_records: u64,
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
}

impl ProducerStat {
    pub(crate) fn new(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        total_stats: Arc<TotalStats>,
        histogram: Arc<Mutex<Histogram<u64>>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
    ) -> Self {
        Self::track_latency(
            num_records,
            end_sender,
            histogram.clone(),
            event_receiver,
            total_stats.clone(),
        );

        Self {}
    }

    fn track_latency(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        histogram: Arc<Mutex<Histogram<u64>>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
        total_stats: Arc<TotalStats>,
    ) {
        spawn(async move {
            let hist = histogram.clone();
            while let Ok(event) = event_receiver.recv().await {
                let hist = hist.clone();
                let stats = total_stats.clone();
                spawn(async move {
                    if stats.first_start_time.read().await.is_none() {
                        stats
                            .first_start_time
                            .write()
                            .await
                            .replace(event.created_at);
                    }
                    let mut hist = hist.lock().await;
                    hist.record(event.elapsed.as_nanos() as u64)
                        .expect("record");

                    stats
                        .record_send
                        .fetch_add(event.records_len, std::sync::atomic::Ordering::Relaxed);
                    stats
                        .record_bytes
                        .fetch_add(event.bytes_size, std::sync::atomic::Ordering::Relaxed);
                });
            }

            // send end
            let record_send = total_stats
                .record_send
                .load(std::sync::atomic::Ordering::Relaxed);
            if record_send >= num_records {
                let record_bytes = total_stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                let hist = hist.lock().await;
                let elapsed = total_stats
                    .first_start_time
                    .read()
                    .await
                    .expect("start time")
                    .elapsed();

                let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;
                let records_per_sec = (record_send as f64 / elapsed_seconds).round() as u64;
                let bytes_per_sec = (record_bytes as f64 / elapsed_seconds).round() as u64;

                let end = EndProducerStat {
                    histogram: hist.clone(),
                    total_records: record_send,
                    records_per_sec,
                    bytes_per_sec,
                };
                end_sender.send(end).await.expect("send end");
            }
        });
    }
}

pub(crate) struct StatCollector {
    total_stats: Arc<TotalStats>,
    num_records: u64,
    end_sender: Sender<EndProducerStat>,
    histogram: Arc<Mutex<Histogram<u64>>>,
}

impl StatCollector {
    pub(crate) fn create(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        stats_sender: Sender<Stats>,
    ) -> Self {
        let histogram = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new(3).unwrap()));

        let total_stats = Arc::new(TotalStats {
            record_send: AtomicU64::new(0),
            record_bytes: AtomicU64::new(0),
            first_start_time: RwLock::new(None),
        });

        Self::send_stats(histogram.clone(), stats_sender.clone(), total_stats.clone())
            .expect("send stats");

        Self {
            total_stats,
            num_records,
            end_sender,
            histogram,
        }
    }

    pub(crate) fn add_producer(&self, event_receiver: Receiver<ProduceCompletionBatchEvent>) {
        ProducerStat::new(
            self.num_records,
            self.end_sender.clone(),
            self.total_stats.clone(),
            self.histogram.clone(),
            event_receiver,
        );
    }

    fn send_stats(
        histogram: Arc<Mutex<Histogram<u64>>>,
        stats_sender: Sender<Stats>,
        total_stats: Arc<TotalStats>,
    ) -> Result<(), std::io::Error> {
        spawn(async move {
            loop {
                let stats_sender = stats_sender.clone();
                let total_stats = Arc::clone(&total_stats);
                let old_record_send = total_stats
                    .record_send
                    .load(std::sync::atomic::Ordering::Relaxed);
                let old_record_bytes = total_stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                sleep(Duration::from_secs(1)).await;
                let first_start_time = total_stats.first_start_time.read().await;
                if first_start_time.is_none() {
                    continue;
                }

                let new_record_send = total_stats
                    .record_send
                    .load(std::sync::atomic::Ordering::Relaxed);
                let new_record_bytes = total_stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);

                if new_record_send == old_record_send {
                    continue;
                }

                let record_send = new_record_send - old_record_send;
                let record_bytes = new_record_bytes - old_record_bytes;
                let elapsed = first_start_time.expect("start time").elapsed();
                let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;
                let records_per_sec = (new_record_send as f64 / elapsed_seconds).round() as u64;
                let bytes_per_sec = (new_record_bytes as f64 / elapsed_seconds).round() as u64;

                let hist = histogram.lock().await;
                let latency_avg = hist.mean() as u64;
                let latency_max = hist.value_at_quantile(1.0);

                stats_sender
                    .send(Stats {
                        record_send,
                        record_bytes,
                        records_per_sec,
                        bytes_per_sec,
                        latency_avg,
                        latency_max,
                    })
                    .await
                    .expect("send stats");
            }
        });
        Ok(())
    }
}
