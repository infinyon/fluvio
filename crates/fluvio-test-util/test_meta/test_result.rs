use std::any::Any;
use std::time::Duration;
use std::fmt::{self, Debug, Display, Formatter};

use hdrhistogram::Histogram;
use comfy_table::{Table, Row, Cell};

#[derive(Debug, Clone)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
    // stats
    pub producer_bytes: u64,
    pub producer_latency_histogram: Histogram<u64>,
    pub producer_num: u64,
    pub consumer_bytes: u64,
    pub consumer_latency_histogram: Histogram<u64>,
    pub consumer_num: u64,
    pub topic_num: u64,
    pub topic_create_latency_histogram: Histogram<u64>,
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
            consumer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
            topic_create_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2)
                .unwrap(),
        }
    }
}

impl TestResult {
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

// TODO: Parse the time scalars into Duration
impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let success_str = format!("{}", self.success);
        let duration_str = format!("{:?}", self.duration);

        let mut basic_results_table = Table::new();

        basic_results_table.add_row(Row::from(vec![Cell::new("Pass?"), Cell::new(success_str)]));

        basic_results_table.add_row(Row::from(vec![
            Cell::new("Duration"),
            Cell::new(duration_str),
        ]));

        // let basic_results_table = Table::new(
        //     "Test Results"
        //     ["Pass?", b->success_str],
        //     ["Duration", duration_str]
        // );

        //let topic_create_latency_avg = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.topic_create_latency_histogram.mean() as u64)
        //);
        //let topic_create_latency_p50 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(
        //        self.topic_create_latency_histogram
        //            .value_at_percentile(50.0)
        //    )
        //);
        //let topic_create_latency_p90 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(
        //        self.topic_create_latency_histogram
        //            .value_at_percentile(90.0)
        //    )
        //);
        //let topic_create_latency_p99 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(
        //        self.topic_create_latency_histogram
        //            .value_at_percentile(99.0)
        //    )
        //);
        //let topic_create_latency_p999 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(
        //        self.topic_create_latency_histogram
        //            .value_at_percentile(99.9)
        //    )
        //);

        //let producer_latency_avg = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.producer_latency_histogram.mean() as u64)
        //);
        //let producer_latency_p50 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(50.0))
        //);
        //let producer_latency_p90 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(90.0))
        //);
        //let producer_latency_p99 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(99.0))
        //);
        //let producer_latency_p999 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.producer_latency_histogram.value_at_percentile(99.9))
        //);

        //let consumer_latency_avg = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.consumer_latency_histogram.mean() as u64)
        //);
        //let consumer_latency_p50 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(50.0))
        //);
        //let consumer_latency_p90 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(90.0))
        //);
        //let consumer_latency_p99 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(99.0))
        //);
        //let consumer_latency_p999 = format!(
        //    "{:.2?}",
        //    Duration::from_nanos(self.consumer_latency_histogram.value_at_percentile(99.9))
        //);

        //let perf_results_header = table!(
        //    [b->"Perf Results"]
        //);

        //let perf_created_table = table!(
        //    [b->"Created", "#"],
        //    ["Topic", self.topic_num],
        //    ["Producer", self.producer_num],
        //    ["Consumer", self.consumer_num]
        //);

        //let perf_latency_table = table!(
        //    [b->"Latency", b->"Average", b->"P50", b->"P90", b->"P99", b->"P999"],
        //    ["Topic create", topic_create_latency_avg, topic_create_latency_p50, topic_create_latency_p90, topic_create_latency_p99, topic_create_latency_p999],
        //    ["Producer", producer_latency_avg, producer_latency_p50, producer_latency_p90, producer_latency_p99, producer_latency_p999],
        //    ["Consumer", consumer_latency_avg, consumer_latency_p50, consumer_latency_p90, consumer_latency_p99, consumer_latency_p999]
        //);

        write!(f, "{basic_results_table}")
        //write!(f, "\n{}", perf_results_header)?;
        //write!(f, "\n{}", perf_created_table)?;
        //write!(f, "\n{}", perf_latency_table)
    }
}
