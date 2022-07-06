use std::any::Any;
use std::time::{SystemTime, Duration};
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;
use fluvio::{TopicProducerConfigBuilder, RecordKey};

use fluvio::stats::{ClientStatsDataCollect, ClientStatsMetric, ClientStatsMetricRaw};
use crate::tests::TestRecordBuilder;

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
}

impl TestOption for StatsTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Test that our external measurements are within 10% of the measurements
const LOW_BOUND: f64 = 0.9;
const HIGH_BOUND: f64 = 1.1;

#[fluvio_test]
pub fn stats(mut test_driver: TestDriver, mut test_case: TestCase) {
    println!("Testing producer stats");
    let test_case: StatsTestCase = test_case.into();

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

            println!("");
            println!("producer batch size: {}", test_case.option.batch_size);

            // Calculate final payload size
            let magic_header_size = 58.0;
            let record_size = record_json.len() as f64 + magic_header_size;

            let max_records_in_batch = (test_case.option.batch_size as f64 / record_size) as u64;
            let max_batch_size = record_size * max_records_in_batch as f64;

            println!("max_batch_size: {max_batch_size}");
            println!("max_records_in_batch: {max_records_in_batch}");

            let mut total_payload_size = 0.0;
            let mut total_records_sent = 0.0;
            let mut total_wait = Duration::new(0, 0);

            // Create producer
            // Configure a connector w/ stats
            let producer_config = TopicProducerConfigBuilder::default()
                .batch_size(test_case.option.batch_size)
                .stats_collect(ClientStatsDataCollect::All)
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
                println!("");
                println!("Round #{r}");
                println!("");

                // Create a larger payload each round
                let payload: Vec<(RecordKey, Vec<u8>)> = (0..r)
                    .into_iter()
                    .map(|_| (RecordKey::NULL, record_json.clone()))
                    .collect();

                let round_payload_size = record_size * r as f64 + magic_header_size;
                total_payload_size += round_payload_size;

                let round_records_sent = r as f64;
                total_records_sent += round_records_sent;

                // start timer
                let round_timer = SystemTime::now();
                producer.send_all(payload).await.unwrap();
                producer.flush().await.unwrap();
                let round_elapsed = round_timer.elapsed().unwrap();
                // stop timer

                total_wait += round_elapsed;

                // Get dataframe check if our calculations are within tolerances

                if let Some(frame) = producer.stats().await {
                    if let ClientStatsMetricRaw::RunTime(n) = frame.get(ClientStatsMetric::RunTime)
                    {
                        let time_check = global_timer.elapsed().unwrap().as_nanos() as f64;
                        let ratio = time_check / n as f64;
                        println!("runtime: {n}");
                        println!("External timer: {:#?}", time_check);
                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }

                    if let ClientStatsMetricRaw::LastRecords(n) =
                        frame.get(ClientStatsMetric::LastRecords)
                    {
                        let ratio = if r.rem_euclid(max_records_in_batch as u32) != 0 {
                            // Calculate records in last batch
                            let record_remain = r.rem_euclid(max_records_in_batch as u32) as f64;

                            println!("# records sent in last batch: {n}");
                            println!("Calc leftover records: {record_remain}");
                            let ratio = record_remain as f64 / n as f64;
                            ratio
                        } else {
                            let ratio = if r.rem_euclid(max_records_in_batch as u32) == 0 {
                                max_records_in_batch as f64 / n as f64
                            } else {
                                round_records_sent / n as f64
                            };

                            println!("last records sent: {n}");
                            println!("round records recorded: {:#?}", round_records_sent);
                            ratio
                        };

                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }

                    if let ClientStatsMetricRaw::Records(n) = frame.get(ClientStatsMetric::Records)
                    {
                        let ratio = total_records_sent / n as f64;
                        println!("total records sent: {n}");
                        println!("total records recorded: {}", total_records_sent);
                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }

                    if let ClientStatsMetricRaw::LastBytes(n) =
                        frame.get(ClientStatsMetric::LastBytes)
                    {
                        let ratio = if r.rem_euclid(max_records_in_batch as u32) != 0 {
                            // Calculate modulus
                            let record_remain = r.rem_euclid(max_records_in_batch as u32) as f64;

                            let payload_remain = (record_size * record_remain) + magic_header_size;

                            println!("last bytes (from full batch): {n}");
                            println!("Calc leftover bytes: {payload_remain}");
                            let ratio = payload_remain as f64 / n as f64;
                            ratio
                        } else {
                            let ratio = if r.rem_euclid(max_records_in_batch as u32) == 0 {
                                max_batch_size as f64 / n as f64
                            } else {
                                round_payload_size as f64 / n as f64
                            };

                            println!("last bytes: {n}");
                            println!("Calc bytes: {round_payload_size}");
                            ratio
                        };

                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }

                    if let ClientStatsMetricRaw::Bytes(n) = frame.get(ClientStatsMetric::Bytes) {
                        let ratio = total_payload_size as f64 / n as f64;

                        println!("total bytes: {n}");
                        println!("Calc bytes: {total_payload_size}");
                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }

                    if let ClientStatsMetricRaw::Throughput(t) =
                        frame.get(ClientStatsMetric::Throughput)
                    {
                        let time_check = global_timer.elapsed().unwrap().as_secs_f64();
                        let calc_throughput = total_payload_size as f64 / time_check;
                        let ratio = calc_throughput / t as f64;

                        println!("total throughput: {t}");
                        println!("Calc : {}", calc_throughput);
                        println!("ratio {}", ratio);
                        assert!(ratio > LOW_BOUND);
                        assert!(ratio < HIGH_BOUND);
                        println!("");
                    }
                }
            }
        },
        "producer"
    );

    let _ = producer_wait.join();
}
