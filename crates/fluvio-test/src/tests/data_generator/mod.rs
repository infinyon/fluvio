pub mod producer;

use core::panic;
use std::any::Any;
use std::time::Duration;
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::{async_process, fork_and_wait};
use tracing::{Instrument, debug_span};

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

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Longevity Test")]
pub struct GeneratorTestOption {
    /// Opt-in to detailed output printed to stdout
    #[clap(long, short)]
    verbose: bool,
}

impl TestOption for GeneratorTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = "generator", topic = "generated", timeout = false)]
pub fn data_generator(test_driver: FluvioTestDriver, test_case: TestCase) {
    let test_case: GeneratorTestCase = test_case.into();

    println!("Starting data generation");

    let expected_runtime = if test_case.environment.timeout != Duration::MAX {
        format!("{:?}", test_case.environment.timeout)
    } else {
        "forever".to_string()
    };

    println!("Expected runtime: {:?}", expected_runtime);

    println!("# Producers: {}", test_case.environment.producer);

    // Batch info
    println!("linger (ms): {:?}", test_case.environment.producer_linger);
    println!(
        "batch size (Bytes): {:?}",
        test_case.environment.producer_batch_size
    );
    println!(
        "compression algorithm: {:?}",
        test_case.environment.producer_compression
    );

    let sync_topic = if let Some(run_id) = &test_case.environment.topic_salt {
        format!("sync-{}", run_id)
    } else {
        "sync".to_string()
    };

    if !test_case.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    // Create topics
    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            #[cfg(feature = "telemetry")]
            let _trace_guard = if test_case.environment.telemetry() {
                fluvio_test_util::setup::init_jaeger!()
            } else {
                None
            };
            let span = debug_span!("data_generator_setup");

            async {
                let mut test_driver_setup = test_driver.clone();

                // Connect test driver to cluster before starting test
                test_driver_setup.connect().instrument(debug_span!("connect_for_setup")).await.expect("Unable to connect to cluster");

                // Create sync topic before starting test
                {
                let mut env_opts = test_case.environment.clone();
                env_opts.topic_name = Some(sync_topic.clone());
                test_driver_setup.create_topic(&env_opts)
                    .instrument(debug_span!("topic_create"))
                    .await
                    .expect("Unable to create default topic");
                }

                test_driver_setup.disconnect();
            }.instrument(span).await

        })
    };

    // Pass run id to producers, so they can select the correct topics
    let mut producer_wait = Vec::new();
    for i in 0..test_case.environment.producer {
        println!("Starting Producer #{}", i);
        let producer = async_process!(
            async {
                #[cfg(feature = "telemetry")]
                let _trace_guard = if test_case.environment.telemetry() {
                    fluvio_test_util::setup::init_jaeger!()
                } else {
                    None
                };

                let span = debug_span!("data_generator_producer");

                async {
                    producer::producer(
                        test_driver.clone(),
                        test_case.clone(),
                        i,
                        test_case.environment.topic_salt.clone(),
                    )
                    .instrument(debug_span!("producer", i = i))
                    .await
                }
                .instrument(span)
                .await
            },
            format!("producer-{}", i)
        );

        producer_wait.push(producer);
    }

    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            #[cfg(feature = "telemetry")]
            let _trace_guard = if test_case.environment.telemetry() {
                fluvio_test_util::setup::init_jaeger!()
            } else {
                None
            };
            let span = debug_span!("data_generator_work_sync");

            async {
                let mut test_driver_setup = test_driver.clone();

                // Connect test driver to cluster before starting test
                test_driver_setup.connect().instrument(debug_span!("connect_for_sync")).await.expect("Unable to connect to cluster");


                println!("setup");

                let sync_consumer = test_driver
                    .get_consumer(&sync_topic, 0)
                    .instrument(debug_span!("consumer_create_for_sync"))
                    .await;

                let mut sync_stream = sync_consumer

                    .stream(Offset::from_end(0))
                    .instrument(debug_span!("stream_create_for_sync"))
                    .await
                    .expect("Unable to open stream");

                let sync_producer = test_driver
                    .create_producer(&sync_topic)
                    .instrument(debug_span!("producer_create_for_sync"))
                    .await;

                // Wait for everyone to get ready
                let mut num_producers = test_case.environment.producer;
                let mut is_ready = false;
                while let Some(Ok(record)) = sync_stream.next().instrument(debug_span!("sync_next")).await {
                    let _key = record
                        .key()
                        .map(|key| String::from_utf8_lossy(key).to_string());
                    let value = String::from_utf8_lossy(record.value()).to_string();

                    if !is_ready {
                        if value.contains("ready") {
                            num_producers -= 1;
                            println!("main: now waiting on {num_producers} to get ready");
                        }

                        let p_ready = test_case.environment.producer - num_producers;

                        if num_producers == 0 {
                            println!("main: all ready, send start");
                            sync_producer.send(RecordKey::NULL, "start").instrument(debug_span!("producer_ready_event_msg", producers_ready = p_ready )).await.unwrap();
                            sync_producer.flush().instrument(debug_span!("event_flush")).await.unwrap();
                            is_ready = true;
                        }
                    } else {

                        // Idea for interactive runs: we can provide live stats here if producers report metrics
                        break;
                    }
                }

                test_driver_setup.disconnect();
        }.instrument(span).await

        })
    };

    let _: Vec<_> = producer_wait
        .into_iter()
        .map(|p| p.join().expect("Producer thread fail"))
        .collect();
}
