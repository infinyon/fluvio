use std::time::{Duration, Instant};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct TestTimer {
    pub start_time: Instant,
    pub duration: Option<Duration>,
}

impl TestTimer {
    pub fn start() -> Self {
        TestTimer {
            start_time: Instant::now(),
            duration: None,
        }
    }

    pub fn stop(&mut self) {
        self.duration = Some(self.start_time.elapsed());
    }

    pub fn duration(&self) -> Duration {
        self.duration.expect("Timer is still running")
    }
}
