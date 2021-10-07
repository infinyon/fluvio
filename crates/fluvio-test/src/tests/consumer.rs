use std::any::Any;
//use std::env;
//use dataplane::record::Record;
use fluvio::consumer::Record;
use structopt::StructOpt;
use futures_lite::stream::StreamExt;
use std::pin::Pin;
use std::time::SystemTime;

use fluvio::{FluvioError, PartitionConsumer, MultiplePartitionConsumer, RecordKey};
use fluvio::Offset;
use crate::tests::TestRecord;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
//use fluvio_future::task::run_block_on;
use fluvio_test_util::async_process;
use fluvio_future::io::Stream;
use tokio::select;

#[derive(Debug, Clone)]
pub struct ConsumerTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConsumerTestOption,
}

impl From<TestCase> for ConsumerTestCase {
    fn from(test_case: TestCase) -> Self {
        let producer_stress_option = test_case
            .option
            .as_any()
            .downcast_ref::<ConsumerTestOption>()
            .expect("ConsumerTestOption")
            .to_owned();
        ConsumerTestCase {
            environment: test_case.environment,
            option: producer_stress_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Consumer Test")]
pub struct ConsumerTestOption {
    /// Num of consumers to create
    #[structopt(long, default_value = "1")]
    pub consumers: u16,

    /// Max records to consume before stopping
    /// Default, stop when end of topic reached
    #[structopt(long, default_value = "0")]
    pub num_records: u16,

    // TODO: These should be mutually exclusive to each other
    /// Offset should be relative to beginning
    #[structopt(long)]
    pub offset_beginning: bool,
    /// Offset should be relative to end
    #[structopt(long)]
    pub offset_end: bool,

    /// Absolute topic offset
    /// use --offset-beginning or --offset-end to refer to relative offsets
    #[structopt(long, default_value = "0")]
    pub offset: i32,

    /// Partition to consume from.
    /// If multiple consumers, they will all use same partition
    // TODO: Support specifying multiple partitions
    #[structopt(long, default_value = "0")]
    pub partition: i32,

    // TODO: This option needs to be mutually exclusive w/ partition
    /// Test should use multi-partition consumer, default all partitions
    #[structopt(long)]
    pub multi_partition: bool,

    // This will need to be mutually exclusive w/ num_records
    //// total time we want the consumer to run, in seconds
    //#[structopt(long, parse(try_from_str = parse_seconds), default_value = "60")]
    //runtime_seconds: Duration,
    /// Opt-in to detailed output printed to stdout
    #[structopt(long, short)]
    verbose: bool,
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

async fn consume_work<S: ?Sized>(mut stream: Pin<Box<S>>, test_case: ConsumerTestCase)
where
    //S: Stream<Item = Result<Record, FluvioError>> + std::marker::Unpin,
    S: Stream<Item = Result<Record, FluvioError>>,
{
    let mut index: i32 = 0;
    let mut records_recvd = 0;

    'consumer_loop: loop {
        // Take a timestamp before record consumed
        let now = SystemTime::now();

        select! {

                //_ = &mut test_timer => {

                //    println!("Consumer stopped. Time's up!\nRecords received: {:?}", records_recvd);
                //    break 'consumer_loop
                //}

                stream_next = stream.next() => {

                    if let Some(Ok(record_json)) = stream_next {
                        records_recvd += 1;
                        let record: TestRecord =
                            serde_json::from_str(std::str::from_utf8(record_json.as_ref()).unwrap())
                                .expect("Deserialize record failed");

                        let _consume_latency = now.elapsed().clone().unwrap().as_nanos();

                        if test_case.option.verbose {
                            println!(
                                "Consuming {:<7} (size {:<5}): consumed CRC: {:<10}",
                                index,
                                record.data.len(),
                                record.crc,
                            );
                        }

                        assert!(record.validate_crc());

                        index += 1;
                    } else {
                        panic!("Stream ended unexpectedly")
                    }
                }
        }
    }
}

async fn get_single_stream(
    consumer: PartitionConsumer,
    offset: Offset,
) -> Pin<Box<dyn Stream<Item = Result<Record, FluvioError>>>> {
    Box::pin(
        consumer
            .stream(offset)
            .await
            .expect("Unable to open stream"),
    )
}

async fn get_multi_stream(
    consumer: MultiplePartitionConsumer,
    offset: Offset,
) -> Pin<Box<dyn Stream<Item = Result<Record, FluvioError>>>> {
    Box::pin(
        consumer
            .stream(offset)
            .await
            .expect("Unable to open stream"),
    )
}

// Default to using the producer test's topic
#[fluvio_test(name = "consumer", topic = "producer-test")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let test_case: ConsumerTestCase = test_case.into();
    let consumers = test_case.option.consumers;
    let partition = test_case.option.partition;
    let is_multi = test_case.option.multi_partition;
    let raw_offset = test_case.option.offset;

    // We'll assume for now that structopt is handling mutual exclusivity
    let offset = if test_case.option.offset_beginning {
        Offset::from_beginning(raw_offset as u32)
    } else if test_case.option.offset_end {
        Offset::from_end(raw_offset as u32)
    } else {
        Offset::absolute(raw_offset.into()).expect("Couldn't create absolute offset")
    };

    println!("\nStarting Consumer test");

    println!("Consumers              : {}", consumers);
    // starting offset
    // consumer type (basically, specify if multi-partition)
    // partition

    // Spawn the consumers
    let mut consumer_wait = Vec::new();
    for n in 0..consumers {
        let consumer = async_process!(async {
            test_driver
                .connect()
                .await
                .expect("Connecting to cluster failed");

            if is_multi {
                let consumer = test_driver
                    .get_all_partitions_consumer(&test_case.environment.topic_name())
                    .await;
                let stream: Pin<Box<dyn Stream<Item = Result<Record, FluvioError>>>> =
                    get_multi_stream(consumer, offset).await;

                consume_work(Box::pin(stream), test_case).await
            } else {
                let consumer = test_driver
                    .get_consumer(&test_case.environment.topic_name(), partition)
                    .await;
                let stream: Pin<Box<dyn Stream<Item = Result<Record, FluvioError>>>> =
                    get_single_stream(consumer, offset).await;

                consume_work(stream, test_case).await
            }
        });

        consumer_wait.push(consumer);
    }

    let _: Vec<_> = consumer_wait
        .into_iter()
        .map(|p| p.join().expect("Consumer thread fail"))
        .collect();
}
