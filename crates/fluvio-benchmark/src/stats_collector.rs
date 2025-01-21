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

pub struct AtomicStats {
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
        stats_sender: Sender<Stats>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
    ) -> Self {
        let stats = Arc::new(AtomicStats {
            record_send: AtomicU64::new(0),
            record_bytes: AtomicU64::new(0),
            first_start_time: RwLock::new(None),
        });

        let histogram = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new(3).unwrap()));

        ProducerStat::track_latency(
            num_records,
            end_sender,
            histogram.clone(),
            event_receiver,
            stats.clone(),
        );

        ProducerStat::send_stats(histogram.clone(), stats_sender, stats.clone())
            .expect("send stats");

        Self {}
    }

    fn send_stats(
        histogram: Arc<Mutex<Histogram<u64>>>,
        stats_sender: Sender<Stats>,
        stats: Arc<AtomicStats>,
    ) -> Result<(), std::io::Error> {
        spawn(async move {
            loop {
                let stats_sender = stats_sender.clone();
                let stats = Arc::clone(&stats);
                let old_record_send = stats.record_send.load(std::sync::atomic::Ordering::Relaxed);
                let old_record_bytes = stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                sleep(Duration::from_secs(1)).await;
                let first_start_time = stats.first_start_time.read().await;
                if first_start_time.is_none() {
                    continue;
                }

                let new_record_send = stats.record_send.load(std::sync::atomic::Ordering::Relaxed);
                let new_record_bytes = stats
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

    fn track_latency(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        histogram: Arc<Mutex<Histogram<u64>>>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
        stats: Arc<AtomicStats>,
    ) {
        spawn(async move {
            let hist = histogram.clone();
            while let Ok(event) = event_receiver.recv().await {
                let hist = hist.clone();
                let stats = stats.clone();
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
            let record_send = stats.record_send.load(std::sync::atomic::Ordering::Relaxed);
            if record_send >= num_records {
                let record_bytes = stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                let hist = hist.lock().await;
                let elapsed = stats
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
    _current: ProducerStat,
}

impl StatCollector {
    pub(crate) fn create(
        num_records: u64,
        end_sender: Sender<EndProducerStat>,
        stat_sender: Sender<Stats>,
        event_receiver: Receiver<ProduceCompletionBatchEvent>,
    ) -> Self {
        Self {
            _current: ProducerStat::new(num_records, end_sender, stat_sender, event_receiver),
        }
    }
}
