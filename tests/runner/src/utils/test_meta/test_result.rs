use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use prettytable::{table, row, cell};
use std::time::Duration;
use hdrhistogram::Histogram;
use charts::PointDatum;

//use plotters::prelude::*;
//const OUT_FILE_NAME: &'static str = "plotters.svg";

#[derive(Debug, Default, Clone)]
pub struct FluvioTimeData(pub u128, pub f32);

impl FluvioTimeData {}

impl PointDatum<f32, f32> for FluvioTimeData {
    fn get_x(&self) -> f32 {
        self.0 as f32
    }

    fn get_y(&self) -> f32 {
        self.1 as f32
    }

    fn get_key(&self) -> String {
        String::new()
    }
}

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
        use charts::{Chart, ScaleLinear, LineSeriesView};

        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        // Start: Producer Latency timeseries

        let x = ScaleLinear::new()
            .set_domain(vec![
                (self.producer_time_latency[0].0 as f32) as f32,
                (self.producer_time_latency.last().unwrap().0 as f32) as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Create a linear scale that will interpolate values in [0, 100] range to corresponding
        // values in [availableHeight, 0] range (the height of the chart without the margins).
        // The [availableHeight, 0] range is inverted because SVGs coordinate system's origin is
        // in top left corner, while chart's origin is in bottom left corner, hence we need to invert
        // the range on Y axis for the chart to display as though its origin is at bottom left.
        let y = ScaleLinear::new()
            .set_domain(vec![
                (self.producer_latency_histogram.min() as f32).log(10.0),
                (self.producer_latency_histogram.max() as f32).log(10.0),
            ])
            .set_range(vec![height - top - bottom, 0]);

        // Going to scale the latency data logarithmically
        let line_data = self
            .producer_time_latency
            .clone()
            .into_iter()
            .map(|x| FluvioTimeData(x.0, (x.1 as f32).log(10.0)))
            .collect();

        println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from("(Unofficial) Producer Latency x Time"))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label("Producer Latency (ms)")
            .add_bottom_axis_label("Test duration (ms)")
            .save("producer-latency-x-time.svg")
            .unwrap();

        // Start: Consumer Latency timeseries

        let x = ScaleLinear::new()
            .set_domain(vec![
                (self.consumer_time_latency[0].0 as f32) as f32,
                (self.consumer_time_latency.last().unwrap().0 as f32) as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Create a linear scale that will interpolate values in [0, 100] range to corresponding
        // values in [availableHeight, 0] range (the height of the chart without the margins).
        // The [availableHeight, 0] range is inverted because SVGs coordinate system's origin is
        // in top left corner, while chart's origin is in bottom left corner, hence we need to invert
        // the range on Y axis for the chart to display as though its origin is at bottom left.
        let y = ScaleLinear::new()
            .set_domain(vec![
                (self.consumer_latency_histogram.min() as f32).log(10.0),
                (self.consumer_latency_histogram.max() as f32).log(10.0),
            ])
            .set_range(vec![height - top - bottom, 0]);

        // Going to scale the latency data logarithmically
        let line_data = self
            .consumer_time_latency
            .clone()
            .into_iter()
            .map(|x| FluvioTimeData(x.0, (x.1 as f32).log(10.0)))
            .collect();

        println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from("(Unofficial) Consumer Latency x Time"))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label("Consumer Latency (ms)")
            .add_bottom_axis_label("Test duration (ms)")
            .save("consumer-latency-x-time.svg")
            .unwrap();

        // Start: E2E Latency timeseries

        let x = ScaleLinear::new()
            .set_domain(vec![
                (self.e2e_time_latency[0].0 as f32) as f32,
                (self.e2e_time_latency.last().unwrap().0 as f32) as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Create a linear scale that will interpolate values in [0, 100] range to corresponding
        // values in [availableHeight, 0] range (the height of the chart without the margins).
        // The [availableHeight, 0] range is inverted because SVGs coordinate system's origin is
        // in top left corner, while chart's origin is in bottom left corner, hence we need to invert
        // the range on Y axis for the chart to display as though its origin is at bottom left.
        let y = ScaleLinear::new()
            .set_domain(vec![
                (self.e2e_latency_histogram.min() as f32).log(10.0),
                (self.e2e_latency_histogram.max() as f32).log(10.0),
            ])
            .set_range(vec![height - top - bottom, 0]);

        // Going to scale the latency data logarithmically
        let line_data = self
            .e2e_time_latency
            .clone()
            .into_iter()
            .map(|x| FluvioTimeData(x.0, (x.1 as f32).log(10.0)))
            .collect();

        println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from("(Unofficial) End-to-end Latency x Time"))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label("e2e Latency (ms)")
            .add_bottom_axis_label("Test duration (ms)")
            .save("e2e-latency-x-time.svg")
            .unwrap();





        write!(f, "{}", basic_results_table)?;
        write!(f, "\n{}", perf_results_header)?;
        write!(f, "\n{}", perf_created_table)?;
        write!(f, "\n{}", perf_throughput_table)?;
        write!(f, "\n{}", perf_latency_table)
    }
}
