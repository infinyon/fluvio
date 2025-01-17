use std::time::Instant;

use async_channel::Sender;

#[derive(Debug)]
pub(crate) struct ProducerStat {
    record_send: u64,
    record_bytes: u64,
    start_time: Instant,
}

impl ProducerStat {
    pub(crate) fn new() -> Self {
        Self {
            record_send: 0,
            record_bytes: 0,
            start_time: Instant::now(),
        }
    }

    pub(crate) fn calcuate(&mut self) -> Stat {
        let elapse = self.start_time.elapsed().as_millis();
        let message_per_sec = ((self.record_send as f64 / elapse as f64) * 1000.0).round();
        let bytes_per_sec = (self.record_bytes as f64 / elapse as f64) * 1000.0;

        Stat {
            message_per_sec,
            bytes_per_sec,
            total_bytes_send: self.record_bytes,
            total_message_send: self.record_send,
            end: false,
        }
    }

    pub(crate) fn set_current_time(&mut self) {
        self.start_time = Instant::now();
    }
}

pub(crate) struct Stat {
    pub message_per_sec: f64,
    pub bytes_per_sec: f64,
    pub total_bytes_send: u64,
    pub total_message_send: u64,
    pub end: bool,
}

pub(crate) struct StatCollector {
    current: ProducerStat,
    batch_size: u64,     // number of records before we calculate stats
    current_record: u64, // how many records we have sent in current cycle
    sender: Sender<Stat>,
}

impl StatCollector {
    pub(crate) fn create(batch_size: u64, sender: Sender<Stat>) -> Self {
        Self {
            current: ProducerStat::new(),
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

    pub(crate) async fn record_record_send(&mut self, bytes: u64) {
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
            message_per_sec: 0.0,
            bytes_per_sec: 0.0,
            total_bytes_send: 0,
            total_message_send: 0,
            end: true,
        };

        self.sender.try_send(end_record).expect("send end stats");
    }
}
