pub mod producer;

use core::panic;
use std::any::Any;
use std::num::ParseIntError;
use std::time::Duration;
use structopt::StructOpt;
use uuid::Uuid;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::{async_process, fork_and_wait};

use fluvio::{Offset, RecordKey};
use futures::StreamExt;

#[derive(Debug, Clone)]
pub struct GeneratorTestCase {
    pub environment: EnvironmentSetup,
    pub option: GeneratorTestOption,
}

impl From<TestCase> for GeneratorTestCase {
    fn from(test_case: TestCase) -> Self {
        let data_generator_option = test_case
            .option
            .as_any()
            .downcast_ref::<GeneratorTestOption>()
            .expect("GeneratorTestOption")
            .to_owned();
        GeneratorTestCase {
            environment: test_case.environment,
            option: data_generator_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Longevity Test")]
pub struct GeneratorTestOption {
    /// Max time we want the producer to run, in seconds - Default: forever
    #[structopt(long, parse(try_from_str = parse_seconds), default_value = "0")]
    runtime_seconds: Duration,

    /// Record payload size used by test (bytes)
    #[structopt(long, default_value = "1000")]
    record_size: usize,

    /// Number of producers to create
    #[structopt(long, default_value = "1")]
    pub producers: u32,

    /// Producer linger (ms)
    #[structopt(long = "linger")]
    pub batch_linger_ms: Option<u64>,

    /// Producer batch size (bytes)
    #[structopt(long)]
    pub batch_size: Option<usize>,

    /// Opt-in to detailed output printed to stdout
    #[structopt(long, short)]
    verbose: bool,

    /// When set, unique topics will be used so multiple
    /// instances of generator against cluster won't collide
    #[structopt(long)]
    multi: bool,
}

fn parse_seconds(s: &str) -> Result<Duration, ParseIntError> {
    let seconds = s.parse::<u64>()?;
    Ok(Duration::from_secs(seconds))
}

impl TestOption for GeneratorTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = "generator", topic = "generated")]
pub fn data_generator(test_driver: FluvioTestDriver, test_case: TestCase) {
    let option: GeneratorTestCase = test_case.into();

    println!("Starting data generation");

    let expected_runtime = if !option.option.runtime_seconds.is_zero() {
        format!("{:?}", option.option.runtime_seconds)
    } else {
        format!("forever")
    };

    println!("Expected runtime: {:?}", expected_runtime);

    println!("# Producers: {}", option.option.producers);

    // Batch info
    println!("linger (ms): {:?}", option.option.batch_linger_ms);
    println!("batch size (Bytes): {:?}", option.option.batch_size);

    // Generate a run id if we're running multiple instances
    let run_id = if option.option.multi {
        Some(Uuid::new_v4().to_simple().to_string())
    } else {
        None
    };

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    // Pass run id to producers, so they can select the correct topics
    let mut producer_wait = Vec::new();
    for i in 0..option.option.producers {
        println!("Starting Producer #{}", i);
        let producer = async_process!(
            async { producer::producer(test_driver.clone(), option, i, run_id.clone()).await },
            format!("producer-{}", i)
        );

        producer_wait.push(producer);
    }

    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            let mut test_driver_setup = test_driver.clone();
            let mut env_opts = option.environment.clone();

            let sync_topic = if let Some(run_id) = &run_id {
                format!("sync-{}", run_id)
            } else {
                format!("sync")
            };

            env_opts.topic_name = Some(sync_topic.clone());


            // Connect test driver to cluster before starting test
            test_driver_setup.connect().await.expect("Unable to connect to cluster");


            // Create sync topic before starting test
            test_driver_setup.create_topic(&env_opts)
                .await
                .expect("Unable to create default topic");


            println!("setup");

            let sync_consumer = test_driver
                .get_consumer(&sync_topic, 0)
                .await;

            let mut sync_stream = sync_consumer
                .stream(Offset::from_end(0))
                .await
                .expect("Unable to open stream");

            let sync_producer = test_driver
                .create_producer(&sync_topic)
                .await;

            // Wait for everyone to get ready
            let mut num_producers = option.option.producers;
            let mut is_ready = false;
            while let Some(Ok(record)) = sync_stream.next().await {
                let _key = record
                    .key()
                    .map(|key| String::from_utf8_lossy(key).to_string());
                let value = String::from_utf8_lossy(record.value()).to_string();

                if !is_ready {
                    if value.contains("ready") {
                        num_producers -= 1;
                        println!("main: now waiting on {num_producers} to get ready");
                    }

                    if num_producers == 0 {
                        println!("main: all ready, send start");
                        sync_producer.send(RecordKey::NULL, "start").await.unwrap();
                        sync_producer.flush().await.unwrap();
                        is_ready = true;
                    }
                } else {

                    // Idea: we can provide live output here if producers report metrics
                    break;
                }
            }

            test_driver_setup.disconnect();

        })
    };

    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
