use std::any::Any;
use std::time::{SystemTime, Duration};
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;
use fluvio::{TopicProducerConfigBuilder, RecordKey};

use crate::tests::TestRecordBuilder;

use comfy_table::{Table, Row, Cell, CellAlignment};
#[derive(Debug, Clone)]
pub struct StatsTestCase {
    pub environment: EnvironmentSetup,
    pub option: StatsTestOption,
}

impl From<TestCase> for StatsTestCase {
    fn from(test_case: TestCase) -> Self {
        let stats_option = test_case
            .option
            .as_any()
            .downcast_ref::<StatsTestOption>()
            .expect("StatsTestOption")
            .to_owned();
        StatsTestCase {
            environment: test_case.environment,
            option: stats_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Stats Test")]
pub struct StatsTestOption {
    /// Total number of rounds
    /// Each round we increase number of records by 1 then send
    #[clap(long, default_value = "100")]
    pub rounds: u32,

    /// Size of dynamic payload portion of test record
    #[clap(long, default_value = "1000")]
    pub record_size: usize,

    /// Batch size of producer
    #[clap(long, default_value = "16384")]
    pub batch_size: usize,

    /// Percentage of tolerated error between calculation and actual
    #[clap(long, default_value = "10.0")]
    pub tolerance: f64,
}

impl TestOption for StatsTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test]
pub fn stats(mut test_driver: TestDriver, mut test_case: TestCase) {
    println!("Testing producer stats");
    let test_case: StatsTestCase = test_case.into();

    // Set the tolerances
    // Test that our external measurements meet reported measurements
    // within a small margin of error
    let tolerance: f64 = test_case.option.tolerance / 100.0;
    let low_bound: f64 = 1.0 - tolerance;
    let high_bound: f64 = 1.0 + tolerance;
    println!("Acceptable margin of error: {:.3}%", tolerance * 100.0);

    let is_pass = create_bound_check_fn(low_bound, high_bound).unwrap();
    let create_result_row = create_format_row_fn(is_pass);

    let producer_wait = async_process!(
        async {
            test_driver
                .connect()
                .await
                .expect("connecting to cluster failed");

            // Generate test data
            let record = TestRecordBuilder::new()
                .with_random_data(test_case.option.record_size)
                .build();
            let record_json = serde_json::to_string(&record)
                .expect("Convert record to json string failed")
                .as_bytes()
                .to_vec();

            println!("producer batch size: {}", test_case.option.batch_size);

            // For calculating final payload size
            let magic_header_size = 58.0;
            let record_size = record_json.len() as f64;

            let max_records_in_batch = (test_case.option.batch_size as f64 / record_size) as u64;
            let max_batch_size = record_size * max_records_in_batch as f64;

            println!("max_batch_size: {max_batch_size}");
            println!("max_records_in_batch: {max_records_in_batch}");

            let mut total_payload_size = 0.0;
            let mut total_records_sent = 0.0;
            let mut total_latency = Duration::new(0, 0);

            // Create producer
            // Configure a connector w/ stats
            let producer_config = TopicProducerConfigBuilder::default()
                .batch_size(test_case.option.batch_size)
                .build()
                .unwrap();
            let producer = test_driver
                .create_producer_with_config(
                    &test_case.environment.topic_name.unwrap(),
                    producer_config,
                )
                .await;

            let global_timer = SystemTime::now();
            for r in 1..(test_case.option.rounds + 1) {
                println!("\nRound #{r}\n");

                let mut round_table = Table::new();
                round_table.load_preset(comfy_table::presets::UTF8_HORIZONTAL_ONLY);

                // Table header
                let mut header = Row::new();
                header.add_cell(Cell::new("Metric name").set_alignment(CellAlignment::Center));
                header.add_cell(Cell::new("Expected value").set_alignment(CellAlignment::Center));
                header.add_cell(Cell::new("Reported value").set_alignment(CellAlignment::Center));
                header.add_cell(Cell::new("Ratio").set_alignment(CellAlignment::Center));
                round_table.add_row(header);

                // Add one more record to payload each round
                let payload: Vec<(RecordKey, Vec<u8>)> = (0..r)
                    .into_iter()
                    .map(|_| (RecordKey::NULL, record_json.clone()))
                    .collect();

                // Calculate the round data
                // Bytes
                let round_payload_size = record_size * r as f64 + magic_header_size;
                total_payload_size += round_payload_size;

                // Records
                let round_records_sent = r as f64;
                total_records_sent += round_records_sent;

                // Batches
                //let round_batches_sent = r as f64;
                //total_batches_sent += round_batches_sent;

                // Latency
                // start timer
                let round_timer = SystemTime::now();
                producer.send_all(payload).await.unwrap();
                producer.flush().await.unwrap();
                let round_elapsed = round_timer.elapsed().unwrap();
                // stop timer

                total_latency += round_elapsed;

                // Validate our calculations are within tolerances to the reported values
                /* 
                if let Some(frame) = producer.stats() {
                    // Print out round report before failing test
                    let mut tolerance_check = false;
                    if let ClientStatsMetricRaw::RunTime(reported) =
                        frame.get(ClientStatsMetric::RunTime)
                    {
                        let expected = global_timer.elapsed().unwrap().as_nanos() as f64;
                        let ratio = expected / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::RunTime,
                            expected,
                            reported as u64,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    if let ClientStatsMetricRaw::LastRecords(reported) =
                        frame.get(ClientStatsMetric::LastRecords)
                    {
                        let expected;

                        if r.rem_euclid(max_records_in_batch as u32) != 0 {
                            // Calculate records in last batch
                            expected = r.rem_euclid(max_records_in_batch as u32) as f64;
                        } else if r.rem_euclid(max_records_in_batch as u32) == 0 {
                            expected = max_records_in_batch as f64;
                        } else {
                            expected = round_records_sent as f64;
                        };

                        let ratio = expected / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::LastRecords,
                            expected,
                            reported,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    if let ClientStatsMetricRaw::Records(reported) =
                        frame.get(ClientStatsMetric::Records)
                    {
                        let expected = total_records_sent;
                        let ratio = expected / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::Records,
                            expected,
                            reported,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    if let ClientStatsMetricRaw::LastBytes(reported) =
                        frame.get(ClientStatsMetric::LastBytes)
                    {
                        let expected;
                        if r.rem_euclid(max_records_in_batch as u32) != 0 {
                            // Calculate the records in last batch
                            let record_remain = r.rem_euclid(max_records_in_batch as u32) as f64;

                            expected = (record_size * record_remain) + magic_header_size;
                        } else if r.rem_euclid(max_records_in_batch as u32) == 0 {
                            expected = max_batch_size;
                        } else {
                            expected = round_payload_size;
                        }

                        let ratio = expected as f64 / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::LastBytes,
                            expected,
                            reported,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    if let ClientStatsMetricRaw::Bytes(reported) =
                        frame.get(ClientStatsMetric::Bytes)
                    {
                        let expected = total_payload_size;
                        let ratio = expected as f64 / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::Bytes,
                            expected,
                            reported,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    if let ClientStatsMetricRaw::Throughput(reported) =
                        frame.get(ClientStatsMetric::Throughput)
                    {
                        let time_check = global_timer.elapsed().unwrap().as_secs_f64();
                        let expected = total_payload_size as f64 / time_check;
                        let ratio = expected as f64 / reported as f64;

                        round_table.add_row(create_result_row(
                            ClientStatsMetric::Throughput,
                            expected,
                            reported,
                        ));

                        tolerance_check = tolerance_check || !is_pass(ratio);
                    }

                    println!("{round_table}");
                    assert!(!tolerance_check);
                }
                */
            }
        },
        "producer"
    );

    let _ = producer_wait.join();
}

// To support user-defined tolerances, we create closures to do bounds checking
// and pass it around to our reporting
fn create_bound_check_fn(
    low_bound: f64,
    high_bound: f64,
) -> Result<impl Fn(f64) -> bool + Copy, ()>
where
{
    assert!(low_bound < high_bound);
    Ok(move |ratio| (ratio > low_bound) && (ratio < high_bound))
}

fn create_ratio_formatter_fn<P>(ratio_checker: P) -> impl Fn(f64) -> String
where
    P: Fn(f64) -> bool,
{
    move |ratio| {
        if ratio_checker(ratio) {
            format!("{ratio:.3}")
        } else {
            format!("* {ratio:.3} *")
        }
    }
}

/* 
fn create_format_row_fn<P>(bounds_checker_fn: P) -> impl Fn(ClientStatsMetric, f64, u64) -> Row
where
    P: Fn(f64) -> bool + Copy,
{
    move |metric, expected, reported| {
        let ratio = expected / reported as f64;

        // Do some setup to handle variable bound checks
        let ratio_formatter = create_ratio_formatter_fn(bounds_checker_fn);

        let mut metric_row = Row::new();
        metric_row.add_cell(Cell::new(metric).set_alignment(CellAlignment::Center));
        metric_row.add_cell(
            Cell::new(format!("{}", expected as u64)).set_alignment(CellAlignment::Center),
        );
        metric_row
            .add_cell(Cell::new(format!("{}", reported)).set_alignment(CellAlignment::Center));
        metric_row.add_cell(Cell::new(ratio_formatter(ratio)).set_alignment(CellAlignment::Center));
        metric_row
    }
}
*/
