use std::time::{Instant, Duration};

pub struct SampleSendHalf {
    pub record: String,
    send_start_time: Option<Instant>,
}

impl SampleSendHalf {
    pub fn new(record: &str) -> Self {
        Self {
            record: record.to_string(),
            send_start_time: None,
        }
    }

    pub fn begin_send(&mut self) {
        self.send_start_time = Some(Instant::now());
    }
}

pub struct SampleRecvHalf {
    record: String,
    recv_end_time: Instant,
}
impl SampleRecvHalf {
    pub fn new(record: String, recv_end_time: Instant) -> Self {
        Self {
            record,
            recv_end_time,
        }
    }
}

impl std::cmp::PartialEq<SampleSendHalf> for SampleRecvHalf {
    fn eq(&self, other: &SampleSendHalf) -> bool {
        self.record == other.record
    }
}
impl std::cmp::PartialEq<SampleRecvHalf> for SampleSendHalf {
    fn eq(&self, other: &SampleRecvHalf) -> bool {
        self.record == other.record
    }
}

impl std::ops::Sub<SampleSendHalf> for SampleRecvHalf {
    type Output = Duration;

    fn sub(self, rhs: SampleSendHalf) -> Self::Output {
        self.recv_end_time - rhs.send_start_time.unwrap()
    }
}
