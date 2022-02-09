pub mod producer;
//pub mod consumer;

use core::panic;
use std::any::Any;
use std::num::ParseIntError;
use std::time::Duration;
use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::{async_process, fork_and_wait};

use fluvio::{Offset, FluvioError, RecordKey};
use futures::StreamExt;

#[derive(Debug, Clone)]
pub struct DataGeneratorTestCase {
    pub environment: EnvironmentSetup,
    pub option: DataGeneratorTestOption,
}

impl From<TestCase> for DataGeneratorTestCase {
    fn from(test_case: TestCase) -> Self {
        let data_generator_option = test_case
            .option
            .as_any()
            .downcast_ref::<DataGeneratorTestOption>()
            .expect("DataGeneratorTestOption")
            .to_owned();
        DataGeneratorTestCase {
            environment: test_case.environment,
            option: data_generator_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Longevity Test")]
pub struct DataGeneratorTestOption {
    // total time we want the producer to run, in seconds
    #[structopt(long, parse(try_from_str = parse_seconds), default_value = "3600")]
    runtime_seconds: Duration,

    // This should be mutually exclusive with runtime_seconds
    // num_records: u32

    // record payload size used by test, in bytes
    #[structopt(long, default_value = "1000")]
    record_size: usize,

    #[structopt(long, default_value = "1")]
    pub producers: u32,

    // Offset the consumer should start from
    //#[structopt(long, default_value = "0")]
    //pub consumer_offset: u32,
    /// Opt-in to detailed output printed to stdout
    #[structopt(long, short)]
    verbose: bool,
}

fn parse_seconds(s: &str) -> Result<Duration, ParseIntError> {
    let seconds = s.parse::<u64>()?;
    Ok(Duration::from_secs(seconds))
}

impl TestOption for DataGeneratorTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test()]
pub fn data_generator(test_driver: FluvioTestDriver, test_case: TestCase) {
    let option: DataGeneratorTestCase = test_case.into();

    println!("Starting Longevity Test");
    println!("Expected runtime: {:?}", option.option.runtime_seconds);
    println!("# Producers: {}", option.option.producers);

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    let mut producer_wait = Vec::new();
    for i in 0..option.option.producers {
        println!("Starting Producer #{}", i);
        let producer = async_process!(
            async { producer::producer(test_driver.clone(), option, i).await },
            format!("producer-{}", i)
        );

        producer_wait.push(producer);
    }

    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            let mut test_driver_setup = test_driver.clone();
            let mut sync_option = option.clone();
            //sync_option.environment.topic_name = Some("asdfg".to_string());


            //async_std::task::sleep(Duration::from_secs(5)).await;

            // Connect test driver to cluster before starting test
            test_driver_setup.connect().await.expect("Unable to connect to cluster");

            // Create topic before starting test
            //test_driver_setup.create_topic(&sync_option.environment)
            //    .await
            //    .expect("Unable to create default topic");
            println!("setup");



            //println!("main: opening consumer to sync");
            let sync_consumer = test_driver
                .get_consumer("sync", 0)
                .await;


            //let mut sync_stream = sync_consumer.stream(Offset::end()).await.unwrap();

            let mut sync_stream = sync_consumer
                .stream(Offset::from_end(0))
                .await
                .expect("Unable to open stream");

            let sync_producer = test_driver
                .create_producer("sync")
                .await;

            println!("Got producer");
            println!("Got producer");

            // Sync starts when this is uncommented, but we want this in the loop...
            //sync_producer.send(RecordKey::NULL, "start").await.unwrap();

            // Wait for everyone to get ready

            let mut num_producers = 1;
            //println!("main: waiting on {num_producers} to get ready");
            while let Some(Ok(record)) = sync_stream.next().await {
                println!("fldjlfks");
                println!("fldjlfks");
                let _key = record
                    .key()
                    .map(|key| String::from_utf8_lossy(key).to_string());
                let value = String::from_utf8_lossy(record.value()).to_string();
                //println!("DEBUG: {value}");
                //println!("DEBUG: {value}");

                if value.contains("ready") {
                    num_producers -= 1;
                    println!("main: now waiting on {num_producers} to get ready");
                }

                if num_producers == 0 {
                    println!("main: all ready, send start");

                    // this isn't sending...
                    sync_producer.send(RecordKey::NULL, "start").await.unwrap();
                    sync_producer.flush().await.unwrap();
                    println!("printprintprint");
                    println!("printprintprint");

                  break;
                }
            }







            // Disconnect test driver to cluster before starting test
            test_driver_setup.disconnect();

        })
    };

    //let _: Vec<_> = consumer_wait
    //    .into_iter()
    //    .map(|c| c.join().expect("Consumer thread fail"))
    //    .collect();
    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
