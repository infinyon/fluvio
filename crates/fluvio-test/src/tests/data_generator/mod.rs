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
#[structopt(name = "Fluvio DataGenerator Test")]
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

    #[structopt(long, default_value = "1")]
    pub consumers: u32,

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

    println!("Starting Data Generator");
    println!("Expected runtime: {:?}", option.option.runtime_seconds);
    //println!("# Consumers: {}", option.option.consumers);
    println!("# Producers: {}", option.option.producers);

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    // Uncommented, this locks
    //let _setup_status = fork_and_wait! {
    //    fluvio_future::task::run_block_on(async {
    //        println!("Setup block start");
    //        let mut test_driver_setup = test_driver.clone();
    //        let mut sync_opt = option.environment.clone();
    //        // Connect test driver to cluster before starting test
    //        test_driver_setup.connect().await.expect("Unable to connect to cluster");


    //        // Create topic before starting test
    //        sync_opt.topic_name = Some("sync".to_string());
    //        test_driver_setup.create_topic(&sync_opt).await.unwrap();
    //        //test_driver_setup.create_topic(&option_setup.environment)
    //        //    .await
    //        //    .expect("Unable to create default topic");

    //        // Disconnect test driver to cluster before starting test
    //        test_driver_setup.disconnect();
    //        println!("Setup block end");
    //    })
    //};

    //let mut consumer_wait = Vec::new();
    //for consumer_id in 0..option.option.consumers {
    //    println!("Starting Consumer #{}", consumer_id);
    //    let consumer = async_process!(
    //        async {
    //            println!("try connecting consumer: {consumer_id}");
    //            test_driver
    //                .connect()
    //                .await
    //                .expect("Connecting to cluster failed");
    //            println!("consumer connected: {consumer_id}");
    //            consumer::consumer_stream(test_driver.clone(), option.clone(), consumer_id).await
    //        },
    //        format!("consumer-{}", consumer_id)
    //    );

    //    consumer_wait.push(consumer);
    //}

    let mut producer_wait = Vec::new();
    for i in 0..option.option.producers {
        println!("Starting Producer #{}", i);
        let producer = async_process!(
            async { producer::producer(test_driver, option, i).await },
            format!("producer-{}", i)
        );

        producer_wait.push(producer);
    }

    //let _: Vec<_> = consumer_wait
    //    .into_iter()
    //    .map(|c| c.join().expect("Consumer thread fail"))
    //    .collect();
    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
