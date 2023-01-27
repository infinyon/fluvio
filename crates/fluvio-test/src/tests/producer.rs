use std::any::Any;
use clap::Parser;

use fluvio::{RecordKey, TopicProducer};
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;
//use tracing::debug;
use hdrhistogram::Histogram;
use std::time::{Duration, SystemTime};
//use std::num::ParseIntError;

#[derive(Debug, Clone)]
pub struct ProducerTestCase {
    pub environment: EnvironmentSetup,
    pub option: ProducerTestOption,
}

impl From<TestCase> for ProducerTestCase {
    fn from(test_case: TestCase) -> Self {
        let producer_stress_option = test_case
            .option
            .as_any()
            .downcast_ref::<ProducerTestOption>()
            .expect("ProducerTestOption")
            .to_owned();
        ProducerTestCase {
            environment: test_case.environment,
            option: producer_stress_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
#[clap(name = "Fluvio Producer Test")]
pub struct ProducerTestOption {
    /// Num of producers to create
    #[clap(long, default_value = "3")]
    pub producers: u32,

    // Not sure how we're going to support this yet
    // max-throughput

    //// total time we want the producer to run, in seconds
    //#[clap(long, value_parser=parse_seconds, default_value = "60")]
    //runtime_seconds: Duration,
    /// Total number of records to producer
    #[clap(long, default_value = "100")]
    pub num_records: u32,

    /// Size of test record. This test doesn't use the TestRecord struct so this is the total size, not just the dynamic payload size
    #[clap(long, default_value = "1000")]
    pub record_size: usize,

    // Eventually we're going to need to support batch options
    // max-linger
    // max-batch
    /// Opt-in to detailed output printed to stdout
    #[clap(long, short)]
    verbose: bool,
}

//fn parse_seconds(s: &str) -> Result<Duration, ParseIntError> {
//    let seconds = s.parse::<u64>()?;
//    Ok(Duration::from_secs(seconds))
//}

impl TestOption for ProducerTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// The total num_records should be divided up and assigned per producer
async fn producer_work(
    producer: TopicProducer,
    producer_id: u32,
    workload_size: u32,
    record_tag_start: u32,
    test_case: ProducerTestCase,
) {
    //println!("Hello, I'm producing {} records", workload_size);

    // Per producer
    // Keep track of latency
    let mut latency_histogram = Histogram::<u64>::new(2).unwrap();
    // Keep track of throughput
    let mut throughput_histogram = Histogram::<u64>::new(2).unwrap();
    // Eventually we'll need to store globally to calculate wrt all producers in test

    // We don't do anything with the record struct so we increase the stress on the system by just sending the same data over and over again.
    // We can only do this because we don't compress the data
    // Otherwise, test-harness spends much of the time allocating the record
    let record = "A".repeat(test_case.option.record_size).as_bytes().to_vec();

    // Loop over num_record
    for record_n in 0..workload_size {
        // This is to report the sent tag relative to the work split
        // Shift the record tag wrt the producer id, so they don't overlap
        let record_tag = record_tag_start + record_n;

        // {
        //  Create record
        //  calculate the min latency to meet throughput requirements
        //
        //  start time=now
        //  send
        //  end time=now
        //
        //  calculate actual latency, throughput
        // }

        let record_size = record.len() as u64;

        //debug!("{:?}", &record);

        // Record the latency
        let now = SystemTime::now();
        producer
            .send(RecordKey::NULL, record.clone())
            .await
            .unwrap_or_else(|_| panic!("Producer {producer_id} send failed"));

        let send_latency = now.elapsed().unwrap().as_nanos() as u64;
        latency_histogram.record(send_latency).unwrap();

        // Calculate send throughput.
        // Starting units: time is in nano, data is in bytes
        // Converting from nanoseconds to seconds, to store (bytes per second) in histogram
        let send_throughput =
            (((record_size as f32) / (send_latency as f32)) * 1_000_000_000.0) as u64;
        throughput_histogram.record(send_throughput).unwrap();

        if test_case.option.verbose {
            // Convert bytes per second to kilobytes per second
            let throughput_kbps = send_throughput / 1_000;

            println!(
                "[producer-{}] record: {:>7} (size {:>5}) Latency: {:>12} Throughput: {:>7?} kB/s",
                producer_id,
                record_tag,
                record_size,
                format!("{:?}", Duration::from_nanos(send_latency)),
                throughput_kbps,
            );
        }
    }

    let produce_p99 = Duration::from_nanos(latency_histogram.value_at_percentile(0.99));

    // Stored as bytes per second. Convert to kilobytes per second
    let throughput_p99 = throughput_histogram.max() / 1_000;

    println!(
        "[producer-{producer_id}] Produce P99: {produce_p99:?} Peak Throughput: {throughput_p99:?} kB/s"
    );
}

#[fluvio_test(name = "producer", topic = "producer-test")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let test_case: ProducerTestCase = test_case.into();
    let total_records = test_case.option.num_records;

    // If we assign more producers than records to split
    // then set # of producers to the # of records
    let producers = if total_records > test_case.option.producers {
        test_case.option.producers
    } else {
        println!("More producers than records to split. Reducing number to {total_records}");
        total_records
    };

    println!("\nStarting Producer test");
    println!("Producers: {producers}");
    println!("# Records: {total_records}");

    // Divide work up amongst the total number of producers
    let record_producer_modulo = total_records % producers;
    let record_producer_div = total_records / producers;
    let even_split = record_producer_div;
    let odd_split = record_producer_div + record_producer_modulo;

    assert_eq!((even_split + record_producer_modulo), odd_split);

    let is_even_split = record_producer_modulo == 0;

    // Spawn the producers
    let mut producer_wait = Vec::new();
    for n in 0..producers {
        println!("Starting Producer #{n}");

        let producer = async_process!(
            async {
                // We want to ensure that we handle the case of an odd split in work
                let workload_size = if n + 1 == producers {
                    if is_even_split {
                        even_split
                    } else {
                        odd_split
                    }
                } else {
                    even_split
                };

                // This is used to print non-overlapping record tags
                let start_record_tag = even_split * n;

                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");

                producer_work(
                    // TODO: Support for multiple topics
                    test_driver
                        .create_producer(&test_case.environment.base_topic_name())
                        .await,
                    n,
                    workload_size,
                    start_record_tag,
                    test_case,
                )
                .await
            },
            format!("producer-{n}")
        );

        producer_wait.push(producer);
    }

    for p in producer_wait {
        p.join().expect("Producer thread fail")
    }
}
