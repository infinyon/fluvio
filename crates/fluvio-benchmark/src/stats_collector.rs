use std::{
    f64,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use fluvio::ProduceCompletionEvent;
use fluvio_future::{sync::Mutex, task::spawn, timer::sleep};
use hdrhistogram::Histogram;

pub(crate) struct ProducerStat {
    stats: Arc<AtomicStats>,
    start_time: Arc<Mutex<Option<Instant>>>,
}

pub struct AtomicStats {
    record_send: AtomicU64,
    record_bytes: AtomicU64,
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
        end_sender: Sender<EndProducerStat>,
        stats_sender: Sender<Stats>,
        event_receiver: Receiver<ProduceCompletionEvent>,
    ) -> Self {
        let start_time = Arc::new(Mutex::new(None));
        let stats = Arc::new(AtomicStats {
            record_send: AtomicU64::new(0),
            record_bytes: AtomicU64::new(0),
        });

        let histogram = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new(3).unwrap()));

        ProducerStat::track_latency(
            end_sender,
            histogram.clone(),
            event_receiver,
            stats.clone(),
            Arc::clone(&start_time),
        );

        ProducerStat::send_stats(histogram.clone(), stats_sender, stats.clone())
            .expect("send stats");

        Self {
            stats: stats.clone(),
            start_time,
        }
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
                let new_record_send = stats.record_send.load(std::sync::atomic::Ordering::Relaxed);
                let new_record_bytes = stats
                    .record_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);

                if new_record_send == old_record_send {
                    continue;
                }

                let records_delta = new_record_send - old_record_send;
                let bytes_delta = new_record_bytes - old_record_bytes;

                let records_per_sec = records_delta;
                let bytes_per_sec = bytes_delta / 1000;

                let hist = histogram.lock().await;
                let latency_avg = hist.mean() as u64;
                let latency_max = hist.value_at_quantile(1.0);

                stats_sender
                    .send(Stats {
                        record_send: records_delta,
                        record_bytes: bytes_delta,
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
        end_sender: Sender<EndProducerStat>,
        histogram: Arc<Mutex<Histogram<u64>>>,
        event_receiver: Receiver<ProduceCompletionEvent>,
        stats: Arc<AtomicStats>,
        start_time: Arc<Mutex<Option<Instant>>>,
    ) {
        spawn(async move {
            let hist = histogram.clone();
            while let Ok(event) = event_receiver.recv().await {
                match event.metadata.base_offset().await {
                    Ok(_base_offset) => {
                        let elapsed = event.created_at.elapsed();
                        let mut hist = hist.lock().await;
                        hist.record(elapsed.as_nanos() as u64).expect("record");
                    }
                    Err(err) => {
                        println!("received err: {:?}", err);
                    }
                }
            }

            // send end
            let hist = hist.lock().await;
            let start_time = start_time.lock().await.expect("start time");
            let elapsed = start_time.elapsed();

            let elapsed_seconds = elapsed.as_millis() as f64 / 1000.0;
            let records_per_sec = (stats.record_send.load(std::sync::atomic::Ordering::Relaxed)
                as f64
                / elapsed_seconds)
                .round() as u64;
            let bytes_per_sec = (stats
                .record_bytes
                .load(std::sync::atomic::Ordering::Relaxed) as f64
                / elapsed_seconds)
                .round() as u64;

            let end = EndProducerStat {
                histogram: hist.clone(),
                total_records: stats.record_send.load(std::sync::atomic::Ordering::Relaxed),
                records_per_sec,
                bytes_per_sec,
            };
            end_sender.send(end).await.expect("send end");
        });
    }

    pub(crate) fn add_record(&mut self, bytes: u64) {
        self.stats
            .record_send
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .record_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) async fn set_current_time(&mut self) {
        self.start_time.lock().await.replace(Instant::now());
    }
}

pub(crate) struct StatCollector {
    current: ProducerStat,
    current_record: u64,
}

impl StatCollector {
    pub(crate) fn create(
        end_sender: Sender<EndProducerStat>,
        stat_sender: Sender<Stats>,
        event_receiver: Receiver<ProduceCompletionEvent>,
    ) -> Self {
        Self {
            current: ProducerStat::new(end_sender, stat_sender, event_receiver),
            current_record: 0,
        }
    }

    pub(crate) async fn start(&mut self) {
        if self.current_record == 0 {
            self.current.set_current_time().await;
        }
    }

    pub(crate) async fn add_record(&mut self, bytes: u64) {
        self.current.add_record(bytes);
        self.current_record += 1;
    }
}
