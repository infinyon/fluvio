use std::any::Any;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use clap::Parser;
use futures_lite::stream::StreamExt;
use tokio::select;
use hdrhistogram::Histogram;

use fluvio_protocol::link::ErrorCode;
use fluvio::consumer::Record;
use fluvio::{ConsumerConfig, MultiplePartitionConsumer, PartitionConsumer, RecordKey};
use fluvio::Offset;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;
use fluvio_future::io::Stream;

use crate::tests::{TestRecord, TestRecordBuilder};

#[derive(Debug, Clone)]
pub struct ConsumerTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConsumerTestOption,
}

impl From<TestCase> for ConsumerTestCase {
    fn from(test_case: TestCase) -> Self {
        let consumer_stress_option = test_case
            .option
            .as_any()
            .downcast_ref::<ConsumerTestOption>()
            .expect("ConsumerTestOption")
            .to_owned();
        ConsumerTestCase {
            environment: test_case.environment,
            option: consumer_stress_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
#[clap(name = "Fluvio Consumer Test")]
pub struct ConsumerTestOption {
    /// Num of consumers to create
    #[clap(long, default_value = "1")]
    pub consumers: u16,

    /// Number of records to send to the topic before running the test
    #[clap(long, default_value = "100")]
    pub num_setup_records: u32,

    /// Size of payload portion of records to send to the topic before running the test
    #[clap(long, default_value = "1000")]
    pub setup_record_size: usize,

    /// Max records to consume before stopping
    /// Default, stop when end of topic reached
    #[clap(long, default_value = "0")]
    pub num_records: u32,

    #[clap(long)]
    pub max_bytes: Option<usize>,

    // TODO: These should be mutually exclusive to each other
    /// Offset should be relative to beginning
    #[clap(long)]
    pub offset_beginning: bool,
    /// Offset should be relative to end
    #[clap(long)]
    pub offset_end: bool,

    /// Absolute topic offset
    /// use --offset-beginning or --offset-end to refer to relative offsets
    #[clap(long, default_value = "0")]
    pub offset: i64,

    /// Partition to consume from.
    /// If multiple consumers, they will all use same partition
    // TODO: Support specifying multiple partitions
    #[clap(long, default_value = "0")]
    pub partition: i32,

    // TODO: This option needs to be mutually exclusive w/ partition
    /// Test should use multi-partition consumer, default all partitions
    #[clap(long)]
    pub multi_partition: bool,

    // This will need to be mutually exclusive w/ num_records
    //// total time we want the consumer to run, in seconds
    //#[clap(long, value_parser=parse_seconds, default_value = "60")]
    //runtime_seconds: Duration,
    /// Opt-in to detailed output printed to stdout
    #[clap(long, short)]
    verbose: bool,

    /// Allow the test to pass if no records are received
    #[clap(long)]
    allow_empty_topic: bool,
}

//fn parse_seconds(s: &str) -> Result<Duration, ParseIntError> {
//    let seconds = s.parse::<u64>()?;
//    Ok(Duration::from_secs(seconds))
//}

impl TestOption for ConsumerTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn consume_work<S: ?Sized>(
    mut stream: Pin<Box<S>>,
    consumer_id: u32,
    test_case: ConsumerTestCase,
) where
    //S: Stream<Item = Result<Record, FluvioError>> + std::marker::Unpin,
    S: Stream<Item = Result<Record, ErrorCode>>,
{
    let mut records_recvd = 0;

    // Per producer
    // Keep track of latency
    let mut latency_histogram = Histogram::<u64>::new(2).unwrap();
    // Keep track of throughput
    let mut throughput_histogram = Histogram::<u64>::new(2).unwrap();

    'consumer_loop: loop {
        // Take a timestamp before record consumed
        let now = SystemTime::now();

        select! {

                //_ = &mut test_timer => {

                //    println!("Consumer stopped. Time's up!\nRecords received: {:?}", records_recvd);
                //    break 'consumer_loop
                //}

                stream_next = stream.next() => {

                    if let Some(Ok(record_raw)) = stream_next {
                        records_recvd += 1;

                        let record_str = std::str::from_utf8(record_raw.as_ref()).unwrap();
                        let record_size = record_str.len();
                        let test_record: TestRecord =
                            serde_json::from_str(record_str)
                                .expect("Deserialize record failed");

                        assert!(test_record.validate_crc());

                        let consume_latency = now.elapsed().clone().unwrap().as_nanos() as u64;
                        latency_histogram.record(consume_latency).unwrap();

                        // Calculate consume throughput.
                        // Starting units: time is in nano, data is in bytes
                        // Converting from nanoseconds to seconds, to store (bytes per second) in histogram
                        let consume_throughput =
                            (((record_size as f32) / (consume_latency as f32)) * 1_000_000_000.0) as u64;
                        throughput_histogram.record(consume_throughput as u64).unwrap();

                        if test_case.option.verbose {
                            println!(
                                "[consumer-{}] record: {:>7} offset: {:>7} (size {:>5}): CRC: {:>10} latency: {:>12} throughput: {:>7?} kB/s",
                                consumer_id,
                                records_recvd,
                                record_raw.offset(),
                                record_size,
                                test_record.crc,
                                format_args!("{:?}", Duration::from_nanos(consume_latency)),
                                (consume_throughput / 1_000)
                            );
                        }

                        if test_case.option.num_records != 0 && records_recvd == test_case.option.num_records  {
                            break 'consumer_loop;
                        }

                    } else {
                        break 'consumer_loop;
                    }

            }
        }
    }

    println!(
        "{} && {} = {}",
        records_recvd,
        !test_case.option.allow_empty_topic,
        records_recvd == 0 && !test_case.option.allow_empty_topic
    );
    if records_recvd == 0 && !test_case.option.allow_empty_topic {
        panic!("Consumer test failed, received no records. If this is intentional, run with --allow-empty-topic");
    }

    let consume_p99 = Duration::from_nanos(latency_histogram.value_at_percentile(0.99));

    // Stored as bytes per second. Convert to kilobytes per second
    let throughput_p99 = throughput_histogram.max() / 1_000;

    println!(
        "[consumer-{}] Consume P99: {:?} Peak Throughput: {:?} kB/s. # Records: {records_recvd}",
        consumer_id, consume_p99, throughput_p99
    );
}

fn build_consumer_config(test_case: ConsumerTestCase) -> ConsumerConfig {
    let mut config = ConsumerConfig::builder();

    // continuous
    if test_case.option.num_records == 0 {
        config.disable_continuous(true);
    }

    // max bytes
    if let Some(max_bytes) = test_case.option.max_bytes {
        config.max_bytes(max_bytes as i32);
    }

    config.build().expect("Couldn't build consumer config")
}

async fn get_single_stream(
    consumer: PartitionConsumer,
    offset: Offset,
    test_case: ConsumerTestCase,
) -> Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>>>> {
    let config = build_consumer_config(test_case);

    Box::pin(
        consumer
            .stream_with_config(offset, config)
            .await
            .expect("Unable to open stream"),
    )
}

async fn get_multi_stream(
    consumer: MultiplePartitionConsumer,
    offset: Offset,
    test_case: ConsumerTestCase,
) -> Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>>>> {
    let config = build_consumer_config(test_case);

    Box::pin(
        consumer
            .stream_with_config(offset, config)
            .await
            .expect("Unable to open stream"),
    )
}

#[fluvio_test(name = "consumer", topic = "consumer-test")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let test_case: ConsumerTestCase = test_case.into();
    let consumers = test_case.option.consumers;
    let partition = test_case.option.partition;
    let is_multi = test_case.option.multi_partition;
    let raw_offset = test_case.option.offset;

    // We'll assume for now that clap is handling mutual exclusivity
    let offset = if test_case.option.offset_beginning {
        Offset::from_beginning(raw_offset)
    } else if test_case.option.offset_end {
        Offset::from_end(raw_offset)
    } else {
        Offset::absolute(raw_offset).expect("Couldn't create absolute offset")
    };

    if test_case.option.num_setup_records != 0 {
        println!(
            "producing {} records for topic {}",
            test_case.option.num_setup_records,
            test_case.environment.base_topic_name()
        );
        // Default producer behaviour round robins between partitions so we don't need to handle the multi-partition case differently
        async_process!(
            async {
                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");

                let producer = test_driver
                    .create_producer(&test_case.environment.base_topic_name())
                    .await;

                let records: Vec<(RecordKey, Vec<u8>)> = (0..test_case.option.num_setup_records)
                    .map(|_| {
                        // Generate test data
                        let record = TestRecordBuilder::new()
                            .with_random_data(test_case.option.setup_record_size)
                            .build();
                        (
                            RecordKey::NULL,
                            serde_json::to_string(&record)
                                .expect("Convert record to json string failed")
                                .as_bytes()
                                .to_vec(),
                        )
                    })
                    .collect();

                producer
                    .send_all(records)
                    .await
                    .expect("failed to send all");
                producer.flush().await.expect("failed to flush");
            },
            format!("consumer-prepopulate-topic")
        )
        .join()
        .expect("Populate records for consumer test failed");
    }

    println!("\nStarting Consumer test");

    println!("Consumers: {}", consumers);
    println!("Starting offset: {:?}", &offset);

    if test_case.option.num_records != 0 {
        println!("# records to consume: {:?}", &test_case.option.num_records);
    } else {
        println!("# records to consume: (until end):");
    }

    // starting offset
    // consumer type (basically, specify if multi-partition)
    // partition

    // Spawn the consumers
    let mut consumer_wait = Vec::new();
    for n in 0..consumers {
        println!("Starting Consumer #{}", n);
        let consumer = async_process!(
            async {
                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");

                // TODO: Support multiple topics

                if is_multi {
                    let consumer = test_driver
                        .get_all_partitions_consumer(&test_case.environment.base_topic_name())
                        .await;
                    let stream: Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>>>> =
                        get_multi_stream(consumer, offset, test_case.clone()).await;

                    consume_work(Box::pin(stream), n.into(), test_case).await
                } else {
                    let consumer = test_driver
                        .get_consumer(&test_case.environment.base_topic_name(), partition)
                        .await;
                    let stream: Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>>>> =
                        get_single_stream(consumer, offset, test_case.clone()).await;

                    consume_work(stream, n.into(), test_case).await
                }
            },
            format!("consumer-{}", n)
        );

        consumer_wait.push(consumer);
    }

    for p in consumer_wait {
        p.join().expect("Consumer thread fail")
    }
}
