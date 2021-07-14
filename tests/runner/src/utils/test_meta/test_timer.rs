use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TestTimer {
    pub start_time: Option<Instant>,
    pub duration: Option<Duration>,
}

impl Default for TestTimer {
    fn default() -> Self {
        TestTimer {
            start_time: None,
            duration: None,
        }
    }
}

impl TestTimer {
    pub fn new() -> Self {
        TestTimer::default()
    }
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        self.duration = None;
    }

    pub fn is_running(&self) -> bool {
        self.start_time.is_some()
    }

    pub fn stop(&mut self) {
        if let Some(time) = self.start_time {
            self.duration = Some(time.elapsed());
        } else {
            self.duration = None;
        }
    }

    pub fn duration(&self) -> Duration {
        self.duration.expect("Timer is still running")
    }

    pub fn elapsed(&self) -> Duration {
        if let Some(time) = self.start_time {
            time.elapsed()
        } else {
            self.duration
                .expect("Timer probably hasn't ever been started")
        }
    }
}
