use std::any::Any;
use structopt::StructOpt;

use fluvio::{RecordKey, TopicProducer};
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use crate::tests::TestRecordBuilder;
use fluvio_test_util::async_process;
use tracing::debug;
use hdrhistogram::Histogram;
use std::time::{Duration, SystemTime};

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

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Producer Test")]
pub struct ProducerTestOption {
    #[structopt(long, default_value = "3")]
    pub producers: u32,
    // duration
    // 0 => never stop, but we're going to need to support signals for clean exit

    // Not sure how we're going to support this yet
    // max-throughput

    // this might eventually be mutually exclusive to duration
    #[structopt(long, default_value = "100")]
    pub num_records: u32,
    #[structopt(long, default_value = "1000")]
    pub record_size: usize,

    // Eventually we're going to need to support batch options
    // max-linger
    // max-batch
    /// Opt-in to detailed output printed to stdout
    #[structopt(long, short)]
    verbose: bool,
}

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
    test_case: ProducerTestCase,
) {
    //println!("Hello, I'm producing {} records", workload_size);

    // Per producer
    // Keep track of latency
    let mut latency_histogram = Histogram::<u64>::new(2).unwrap();
    // Keep track of throughput
    // Eventually we'll need to store globally to calculate wrt all producers in test

    // Loop over num_record
    for record_n in 0..workload_size {
        // {
        //  Create record
        //  calculate the min latency to meet throughput requirements
        //
        //  start time
        //  (dynamic wait based on previous iteration)
        //  send
        //  end time
        //
        //  calculate actual latency, throughput
        // }
        let record = TestRecordBuilder::new()
            .with_tag(format!("{}:{}", producer_id, record_n))
            .with_random_data(test_case.option.record_size)
            .build();
        let record_json = serde_json::to_string(&record)
            .expect("Convert record to json string failed")
            .as_bytes()
            .to_vec();

        debug!("{:?}", &record);

        // Record the latency
        //test_driver
        //    .send_count(&producer, RecordKey::NULL, record_json)
        //    .await
        //    .expect("Producer Send failed");

        let now = SystemTime::now();
        producer
            .send(RecordKey::NULL, record_json)
            .await
            .expect(&format!("Producer {} send failed", producer_id));
        let send_latency = now.elapsed().unwrap().as_nanos() as u64;

        latency_histogram.record(send_latency).unwrap();

        if test_case.option.verbose {
            println!(
                "[producer-{}] Sent (size {:<5}): CRC: {:<10}, Latency: {:?}",
                producer_id,
                record.data.len(),
                record.crc,
                Duration::from_nanos(send_latency),
            );
        }
    }

    println!(
        "[producer-{}] P99: {:?}",
        producer_id,
        Duration::from_nanos(latency_histogram.value_at_percentile(0.99))
    );
}

#[fluvio_test(name = "producer", topic = "producer-test")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let test_case: ProducerTestCase = test_case.into();
    let total_records = test_case.option.num_records;

    // If we assign more producers than records to split
    // then set # of producers to the # of records
    let producers = if total_records > test_case.option.producers.into() {
        test_case.option.producers
    } else {
        println!(
            "More producers than records to split. Reducing number to {}",
            total_records
        );
        total_records
    };

    println!("\nStarting Producer test");

    println!("Producers              : {}", producers);

    println!("# Records              : {}", total_records);

    // Divide work up amongst the total number of producers
    let record_producer_modulo = total_records % producers;
    let record_producer_div = total_records / producers;
    let even_split = record_producer_div;
    let odd_split = record_producer_div + record_producer_modulo;

    assert_eq!((even_split + record_producer_modulo), odd_split);

    let is_even_split = if record_producer_modulo == 0 {
        // if we divide evenly, then no problem

        //println!("each producer will send {} records", even_split);
        true
    } else {
        // otherwise, one producer will deal will take care of the remaining work

        //println!("each producer will send {} records", even_split);
        //println!(
        //    "except for one producer, which will send {} records",
        //    odd_split
        //);

        false
    };

    // Spawn the producers
    let mut producer_wait = Vec::new();
    for n in 0..producers {
        println!("Starting Producer #{}", n + 1);

        let producer = async_process!(async {
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

            test_driver
                .connect()
                .await
                .expect("Connecting to cluster failed");

            producer_work(
                test_driver
                    .create_producer(&test_case.environment.topic_name())
                    .await,
                n,
                workload_size,
                test_case,
            )
            .await
        });

        producer_wait.push(producer);
    }

    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
