use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use prettytable::{table, row, cell};
use std::time::Duration;
use hdrhistogram::Histogram;
use crate::test_meta::chart_builder::{ChartBuilder, FluvioTimeData};

#[derive(Debug, Clone)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
    // stats
    pub producer_bytes: u64,
    pub producer_num: u64,
    pub consumer_bytes: u64,
    pub consumer_num: u64,
    pub topic_num: u64,

    pub topic_create_latency_histogram: Histogram<u64>,
    pub producer_latency_histogram: Histogram<u64>,
    pub producer_time_latency: Vec<FluvioTimeData>,
    pub consumer_latency_histogram: Histogram<u64>,
    pub consumer_time_latency: Vec<FluvioTimeData>,
    pub e2e_latency_histogram: Histogram<u64>,
    pub e2e_time_latency: Vec<FluvioTimeData>,
    pub producer_rate_histogram: Histogram<u64>,
    pub consumer_rate_histogram: Histogram<u64>,
}

impl Default for TestResult {
    fn default() -> Self {
        Self {
            success: false,
            duration: Duration::new(0, 0),

            producer_num: 0,
            consumer_num: 0,
            topic_num: 0,

            producer_bytes: 0,
            consumer_bytes: 0,

            producer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            producer_time_latency: Vec::new(),
            consumer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_time_latency: Vec::new(),
            e2e_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            e2e_time_latency: Vec::new(),
            topic_create_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2)
                .unwrap(),
            producer_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            consumer_rate_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
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
            Duration::from_nanos(self.producer_latency_histogram.mean() as u64)
        );
        let producer_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(50.0))
        );
        let producer_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(90.0))
        );
        let producer_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(99.0))
        );
        let producer_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(99.9))
        );

        const BYTES_IN_MBYTE: f64 = 1_000_000.0;
        let producer_rate_mbps = format!(
            "{:.2?}",
            self.producer_rate_histogram.max() as f64 / BYTES_IN_MBYTE
        );

        let consumer_latency_avg = format!(
            "{:.2?}",
            Duration::from_nanos(self.consumer_latency_histogram.mean() as u64)
        );
        let consumer_latency_p50 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(50.0))
        );
        let consumer_latency_p90 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(90.0))
        );
        let consumer_latency_p99 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(99.0))
        );
        let consumer_latency_p999 = format!(
            "{:.2?}",
            Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(99.9))
        );
        let consumer_rate_mbps = format!(
            "{:.2?}",
            self.consumer_rate_histogram.max() as f64 / BYTES_IN_MBYTE
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
            ["Topic", self.topic_num],
            ["Producer", self.producer_num],
            ["Consumer", self.consumer_num]
        );

        let perf_throughput_table = table!(
            [b->"Throughput", b->"Bytes", b->"Max rate (MBytes/s)"],
            ["Producer", self.producer_bytes, producer_rate_mbps],
            ["Consumer", self.consumer_bytes, consumer_rate_mbps]
        );

        let perf_latency_table = table!(
            [b->"Latency", b->"Average", b->"P50", b->"P90", b->"P99", b->"P999"],
            ["Topic create", topic_create_latency_avg, topic_create_latency_p50, topic_create_latency_p90, topic_create_latency_p99, topic_create_latency_p999],
            ["Producer", producer_latency_avg, producer_latency_p50, producer_latency_p90, producer_latency_p99, producer_latency_p999],
            ["Consumer", consumer_latency_avg, consumer_latency_p50, consumer_latency_p90, consumer_latency_p99, consumer_latency_p999],
            ["E2E", e2e_latency_avg, e2e_latency_p50, e2e_latency_p90, e2e_latency_p99, e2e_latency_p999]
        );

        // TODO: Make chart generation opt-in

        // Producer Latency timeseries
        ChartBuilder::data_x_time(
            self.producer_time_latency.clone(),
            self.producer_latency_histogram.clone(),
            "(Unofficial) Producer Latency x Time",
            "Producer Latency (ms)",
            "Test duration (ms)",
            "producer-latency-x-time.svg",
        );

        // Consumer Latency timeseries
        ChartBuilder::data_x_time(
            self.consumer_time_latency.clone(),
            self.consumer_latency_histogram.clone(),
            "(Unofficial) Consumer Latency x Time",
            "Consumer Latency (ms)",
            "Test duration (ms)",
            "consumer-latency-x-time.svg",
        );

        // E2E Latency timeseries
        ChartBuilder::data_x_time(
            self.e2e_time_latency.clone(),
            self.e2e_latency_histogram.clone(),
            "(Unofficial) End-to-end Latency x Time",
            "E2E Latency (ms)",
            "Test duration (ms)",
            "e2e-latency-x-time.svg",
        );

        write!(f, "{}", basic_results_table)?;
        write!(f, "\n{}", perf_results_header)?;
        write!(f, "\n{}", perf_created_table)?;
        write!(f, "\n{}", perf_throughput_table)?;
        write!(f, "\n{}", perf_latency_table)
    }
}
