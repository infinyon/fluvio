pub mod producer;
pub mod consumer;

use core::panic;
use std::any::Any;
use clap::Parser;
use tracing::{Instrument, debug_span};

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

#[derive(Debug, Clone)]
pub struct LongevityTestCase {
    pub environment: EnvironmentSetup,
    pub option: LongevityTestOption,
}

impl From<TestCase> for LongevityTestCase {
    fn from(test_case: TestCase) -> Self {
        let longevity_option = test_case
            .option
            .as_any()
            .downcast_ref::<LongevityTestOption>()
            .expect("LongevityTestOption")
            .to_owned();
        LongevityTestCase {
            environment: test_case.environment,
            option: longevity_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Longevity Test")]
pub struct LongevityTestOption {
    // This should be mutually exclusive with runtime_seconds
    // num_records: u32

    // Offset the consumer should start from
    //#[clap(long, default_value = "0")]
    //pub consumer_offset: u32,
    /// Opt-in to detailed output printed to stdout
    #[clap(long, short)]
    verbose: bool,
}

impl TestOption for LongevityTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// TODO: Need to add producer + consumer support for multiple topics
#[fluvio_test(topic = "longevity")]
pub fn longevity(test_driver: FluvioTestDriver, test_case: TestCase) {
    //println!("DEBUG: {:#?}", test_case);
    let test_case: LongevityTestCase = test_case.into();

    println!("Starting Longevity Test");
    println!("Expected runtime: {:?}", test_case.environment.timeout());
    println!("# Topics: {}", test_case.environment.topic);
    println!("# Consumers: {}", test_case.environment.consumer);
    println!("# Producers: {}", test_case.environment.producer);

    if !test_case.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    let mut consumer_wait = Vec::new();
    for consumer_id in 0..test_case.environment.consumer {
        println!("Starting Consumer #{}", consumer_id);
        let consumer = async_process!(
            async {
                #[cfg(feature = "telemetry")]
                let _trace_guard = if test_case.environment.telemetry() {
                    fluvio_test_util::setup::init_jaeger!()
                } else {
                    None
                };
                let span = debug_span!("longevity_consumer_{consumer_id}");

                async {
                    println!("try connecting consumer: {consumer_id}");
                    test_driver
                        .connect()
                        .instrument(debug_span!("connect_consumer", consumer_id = consumer_id))
                        .await
                        .expect("Connecting to cluster failed");
                    println!("consumer connected: {consumer_id}");
                    consumer::consumer_stream(test_driver.clone(), test_case.clone(), consumer_id)
                        .instrument(debug_span!("stream_consumer", consumer_id = consumer_id))
                        .await
                }
                .instrument(span)
                .await
            },
            format!("consumer-{}", consumer_id)
        );

        consumer_wait.push(consumer);
    }

    let mut producer_wait = Vec::new();
    for producer_id in 0..test_case.environment.producer {
        println!("Starting Producer #{}", producer_id);
        let producer = async_process!(
            async {
                #[cfg(feature = "telemetry")]
                let _trace_guard = if test_case.environment.telemetry() {
                    fluvio_test_util::setup::init_jaeger!()
                } else {
                    None
                };
                let span = debug_span!("longevity_producer_{producer_id}");

                async {
                    test_driver
                        .connect()
                        .instrument(debug_span!("connect_producer", producer_id = producer_id))
                        .await
                        .expect("Connecting to cluster failed");
                    producer::producer(test_driver, test_case, producer_id)
                        .instrument(debug_span!("start_producer", producer_id = producer_id))
                        .await
                }
                .instrument(span)
                .await
            },
            format!("producer-{}", producer_id)
        );

        producer_wait.push(producer);
    }

    let _: Vec<_> = consumer_wait
        .into_iter()
        .map(|c| c.join().expect("Consumer thread fail"))
        .collect();
    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
