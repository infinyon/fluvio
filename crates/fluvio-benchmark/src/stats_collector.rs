use std::{sync::Arc, time::Instant};

use async_channel::Sender;
use fluvio::ProduceOutput;
use fluvio_future::{sync::Mutex, task::spawn};
use hdrhistogram::Histogram;

#[derive(Debug)]
pub(crate) struct ProducerStat {
    record_send: u64,
    record_bytes: u64,
    start_time: Instant,
    output_tx: Sender<(ProduceOutput, Instant)>,
    histogram: Arc<Mutex<Histogram<u64>>>,
}

impl ProducerStat {
    pub(crate) fn new(num_records: u64, latency_sender: Sender<Histogram<u64>>) -> Self {
        let (output_tx, rx) = async_channel::unbounded::<(ProduceOutput, Instant)>();
        let histogram = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new(2).unwrap()));

        ProducerStat::track_latency(num_records, latency_sender, rx, histogram.clone());

        Self {
            record_send: 0,
            record_bytes: 0,
            start_time: Instant::now(),
            output_tx,
            histogram,
        }
    }

    fn track_latency(
        num_records: u64,
        latency_sender: Sender<Histogram<u64>>,
        rx: async_channel::Receiver<(ProduceOutput, Instant)>,
        histogram: Arc<Mutex<Histogram<u64>>>,
    ) {
        spawn(async move {
            while let Ok((send_out, time)) = rx.recv().await {
                let hist = histogram.clone();
                let latency_sender = latency_sender.clone();
                //spawn(async move {
                match send_out.wait().await {
                    Ok(_) => {
                        let duration = time.elapsed();
                        let mut hist = hist.lock().await;
                        hist.record(duration.as_nanos() as u64).expect("record");

                        if hist.len() >= num_records {
                            latency_sender.send(hist.clone()).await.expect("send");
                        }
                    }
                    Err(err) => {
                        println!("error sending record: {}", err);
                        return;
                    }
                }
                //});
            }
        });
    }

    pub(crate) fn calcuate(&mut self) -> Stat {
        let elapse = self.start_time.elapsed().as_millis();
        let records_per_sec = ((self.record_send as f64 / elapse as f64) * 1000.0).round();
        let bytes_per_sec = (self.record_bytes as f64 / elapse as f64) * 1000.0;

        let hist = self.histogram.lock_blocking();
        let latency_avg = hist.mean() as u64;
        let latency_max = hist.value_at_quantile(1.0);

        Stat {
            records_per_sec,
            bytes_per_sec,
            _total_bytes_send: self.record_bytes,
            total_records_send: self.record_send,
            latency_avg,
            latency_max,
            _end: false,
        }
    }

    pub(crate) fn set_current_time(&mut self) {
        self.start_time = Instant::now();
    }

    pub(crate) fn send_out(&mut self, out: (ProduceOutput, Instant)) {
        self.output_tx.try_send(out).expect("send out");
    }
}

pub(crate) struct Stat {
    pub records_per_sec: f64,
    pub bytes_per_sec: f64,
    pub _total_bytes_send: u64,
    pub total_records_send: u64,
    pub latency_avg: u64,
    pub latency_max: u64,
    pub _end: bool,
}

pub(crate) struct StatCollector {
    current: ProducerStat,
    batch_size: u64,     // number of records before we calculate stats
    current_record: u64, // how many records we have sent in current cycle
    sender: Sender<Stat>,
}

impl StatCollector {
    pub(crate) fn create(
        batch_size: u64,
        num_records: u64,
        latency_sender: Sender<Histogram<u64>>,
        sender: Sender<Stat>,
    ) -> Self {
        Self {
            current: ProducerStat::new(num_records, latency_sender),
            batch_size,
            current_record: 0,
            sender,
        }
    }

    pub(crate) fn start(&mut self) {
        if self.current_record == 0 {
            self.current.set_current_time();
        }
    }

    pub(crate) fn send_out(&mut self, out: (ProduceOutput, Instant)) {
        self.current.send_out(out);
    }

    pub(crate) async fn add_record(&mut self, bytes: u64) {
        self.current.record_send += 1;
        self.current.record_bytes += bytes;
        self.current_record += 1;

        if self.current_record >= self.batch_size {
            let stat = self.current.calcuate();
            self.current_record = 0;
            self.current.record_bytes = 0;
            self.current.record_send = 0;
            self.sender.try_send(stat).expect("send stats");
        }
    }

    pub(crate) fn finish(&mut self) {
        let end_record = Stat {
            records_per_sec: 0.0,
            bytes_per_sec: 0.0,
            _total_bytes_send: 0,
            total_records_send: 0,
            latency_avg: 0,
            latency_max: 0,
            _end: true,
        };

        self.sender.try_send(end_record).expect("send end stats");
    }
}
