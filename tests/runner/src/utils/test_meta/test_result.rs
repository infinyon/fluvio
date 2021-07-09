use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use prettytable::{table, row, cell};
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
    // stats
    pub bytes_produced: u64,
    pub produce_latency: u64,
    pub num_producers: u64,
    pub bytes_consumed: u64,
    pub consume_latency: u64,
    pub num_consumers: u64,
    pub num_topics: u64,
    pub topic_create_latency: u64,
}

impl TestResult {
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn success(self, success: bool) -> Self {
        let mut test_result = self.clone();

        test_result.success = success;
        test_result
    }

    pub fn duration(self, duration: Duration) -> Self {
        let mut test_result = self.clone();

        test_result.duration = duration;
        test_result
    }
}

// TODO: Parse the time scalars into Duration
impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let success_str = format!("{}", self.success);
        let duration_str = format!("{:?}", self.duration);
        let producer_latency_str = format!("{:?}", Duration::from_nanos(self.produce_latency));
        let consumer_latency_str = format!("{:?}", Duration::from_nanos(self.consume_latency));
        let topic_create_latency_str =
            format!("{:?}", Duration::from_nanos(self.topic_create_latency));

        let table = table!(
            [b->"Test Results"],
            ["Pass?", b->success_str],
            ["Duration", duration_str],
            //

            ["# topics created", self.num_topics],
            ["topic create latency 99%", topic_create_latency_str],
            ["# producers created", self.num_producers],
            ["bytes produced", self.bytes_produced],
            ["producer latency 99%", producer_latency_str],
            ["# consumers created", self.num_consumers],
            ["bytes consumed", self.bytes_consumed],
            ["consumer latency 99%", consumer_latency_str]
        );

        write!(f, "{}", table)
    }
}
