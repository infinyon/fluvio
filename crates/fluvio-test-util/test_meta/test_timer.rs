use std::time::{Duration, Instant};
use std::fmt::Debug;
use tracing::instrument;
use tracing_futures::Instrument;

#[derive(Debug, Clone)]
pub struct TestTimer {
    pub start_time: Instant,
    pub duration: Option<Duration>,
}

impl TestTimer {
    #[instrument(level = "trace")]
    pub fn start() -> Self {
        TestTimer {
            start_time: Instant::now(),
            duration: None,
        }
    }

    #[instrument(level = "trace")]
    pub fn stop(&mut self) {
        self.duration = Some(self.start_time.elapsed());
    }

    #[instrument(level = "trace")]
    pub fn duration(&self) -> Duration {
        self.duration.expect("Timer is still running")
    }
}
