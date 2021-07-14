use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use prettytable::{table, row, cell};
use std::time::Duration;
use hdrhistogram::Histogram;

#[derive(Debug, Clone)]
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

    pub topic_create_latency_histogram: Histogram<u64>,
    pub produce_latency_histogram: Histogram<u64>,
    pub consume_latency_histogram: Histogram<u64>,
    pub e2e_latency_histogram: Histogram<u64>,
    pub produce_rate_histogram: Histogram<u64>,
    pub consume_rate_histogram: Histogram<u64>,
}

impl Default for TestResult {
    fn default() -> Self {
        Self {
            success: false,
            duration: Duration::new(0, 0),

            num_producers: 0,
            num_consumers: 0,
            num_topics: 0,

            bytes_produced: 0,
            bytes_consumed: 0,

            produce_latency: 0,
            consume_latency: 0,
            topic_create_latency: 0,

            produce_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consume_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            e2e_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            topic_create_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2)
                .unwrap(),
            produce_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consume_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
        }
    }
}
impl TestResult {
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn success(self, success: bool) -> Self {
        let mut test_result = self;

        test_result.success = success;
        test_result
    }

    pub fn duration(self, duration: Duration) -> Self {
        let mut test_result = self;

        test_result.duration = duration;
        test_result
    }
}

impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let success_str = format!("{}", self.success);
        let duration_str = format!("{:?}", self.duration);

        let basic_results_table = table!(
            [b->"Test Results"],
            ["Pass?", b->success_str],
            ["Duration", duration_str]
        );

        let topic_create_latency_avg = format!(
            "{:.2?}",
            Duration::from_nanos(self.topic_create_latency_histogram.mean() as u64)
        );
        let topic_create_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(
                self.topic_create_latency_histogram
                    .value_at_percentile(50.0)
            )
        );
        let topic_create_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(
                self.topic_create_latency_histogram
                    .value_at_percentile(90.0)
            )
        );
        let topic_create_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(
                self.topic_create_latency_histogram
                    .value_at_percentile(99.0)
            )
        );
        let topic_create_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(
                self.topic_create_latency_histogram
                    .value_at_percentile(99.9)
            )
        );

        let producer_latency_avg = format!(
            "{:.2?}",
            Duration::from_nanos(self.produce_latency_histogram.mean() as u64)
        );
        let producer_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(self.produce_latency_histogram.value_at_percentile(50.0))
        );
        let producer_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(self.produce_latency_histogram.value_at_percentile(90.0))
        );
        let producer_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(self.produce_latency_histogram.value_at_percentile(99.0))
        );
        let producer_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(self.produce_latency_histogram.value_at_percentile(99.9))
        );

        const BYTES_IN_MBYTE: f64 = 1_000_000.0;
        let producer_rate_mbps = format!(
            "{:.2?}",
            self.produce_rate_histogram.max() as f64 / BYTES_IN_MBYTE
        );

        let consumer_latency_avg = format!(
            "{:.2?}",
            Duration::from_nanos(self.consume_latency_histogram.mean() as u64)
        );
        let consumer_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consume_latency_histogram.value_at_percentile(50.0))
        );
        let consumer_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consume_latency_histogram.value_at_percentile(90.0))
        );
        let consumer_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consume_latency_histogram.value_at_percentile(99.0))
        );
        let consumer_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consume_latency_histogram.value_at_percentile(99.9))
        );
        let consumer_rate_mbps = format!(
            "{:.2?}",
            self.consume_rate_histogram.max() as f64 / BYTES_IN_MBYTE
        );

        let e2e_latency_avg = format!(
            "{:.2?}",
            Duration::from_nanos(self.e2e_latency_histogram.mean() as u64)
        );
        let e2e_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(self.e2e_latency_histogram.value_at_percentile(50.0))
        );
        let e2e_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(self.e2e_latency_histogram.value_at_percentile(90.0))
        );
        let e2e_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(self.e2e_latency_histogram.value_at_percentile(99.0))
        );
        let e2e_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(self.e2e_latency_histogram.value_at_percentile(99.9))
        );

        let perf_results_header = table!(
            [b->"Perf Results"]
        );

        let perf_created_table = table!(
            [b->"Created", "#"],
            ["Topic", self.num_topics],
            ["Producer", self.num_producers],
            ["Consumer", self.num_consumers]
        );

        let perf_throughput_table = table!(
            [b->"Throughput", b->"Bytes", b->"Max rate (MBytes/s)"],
            ["Producer", self.bytes_produced, producer_rate_mbps],
            ["Consumer", self.bytes_consumed, consumer_rate_mbps]
        );

        let perf_latency_table = table!(
            [b->"Latency", b->"Average", b->"P50", b->"P90", b->"P99", b->"P999"],
            ["Topic create", topic_create_latency_avg, topic_create_latency_p50, topic_create_latency_p90, topic_create_latency_p99, topic_create_latency_p999],
            ["Producer", producer_latency_avg, producer_latency_p50, producer_latency_p90, producer_latency_p99, producer_latency_p999],
            ["Consumer", consumer_latency_avg, consumer_latency_p50, consumer_latency_p90, consumer_latency_p99, consumer_latency_p999],
            ["E2E", e2e_latency_avg, e2e_latency_p50, e2e_latency_p90, e2e_latency_p99, e2e_latency_p999]
        );

        write!(f, "{}", basic_results_table)?;
        write!(f, "\n{}", perf_results_header)?;
        write!(f, "\n{}", perf_created_table)?;
        write!(f, "\n{}", perf_throughput_table)?;
        write!(f, "\n{}", perf_latency_table)
    }
}
