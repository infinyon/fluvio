pub mod producer;

use core::panic;
use std::time::Duration;
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;
use fluvio_test_util::{async_process, fork_and_wait};

use fluvio::{Offset, RecordKey};
use futures::StreamExt;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[clap(name = "Fluvio Longevity Test")]
pub struct GeneratorTestOption {
    /// Opt-in to detailed output printed to stdout
    #[clap(long, short)]
    verbose: bool,
}

#[fluvio_test(name = "generator", topic = "generated", timeout = false)]
pub fn data_generator(test_driver: FluvioTestDriver, test_case: TestCase) {
    let option: MyTestCase = test_case.into();

    println!("Starting data generation");

    let expected_runtime = if option.environment.timeout != Duration::MAX {
        format!("{:?}", option.environment.timeout)
    } else {
        "forever".to_string()
    };

    println!("Expected runtime: {expected_runtime:?}");

    println!("# Producers: {}", option.environment.producer);

    // Batch info
    println!("linger (ms): {:?}", option.environment.producer_linger);
    println!(
        "batch size (Bytes): {:?}",
        option.environment.producer_batch_size
    );
    println!(
        "compression algorithm: {:?}",
        option.environment.producer_compression
    );

    let sync_topic = if let Some(run_id) = &option.environment.topic_salt {
        format!("sync-{run_id}")
    } else {
        "sync".to_string()
    };

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    // Create topics
    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            let mut test_driver_setup = test_driver.clone();

            // Connect test driver to cluster before starting test
            test_driver_setup.connect().await.expect("Unable to connect to cluster");

            // Create sync topic before starting test
            {
            let mut env_opts = option.environment.clone();
            env_opts.topic_name = Some(sync_topic.clone());
            test_driver_setup.create_topic(&env_opts)
                .await
                .expect("Unable to create default topic");
            }

            test_driver_setup.disconnect();

        })
    };

    // Pass run id to producers, so they can select the correct topics
    let mut producer_wait = Vec::new();
    for i in 0..option.environment.producer {
        println!("Starting Producer #{i}");
        let producer = async_process!(
            async {
                producer::producer(
                    test_driver.clone(),
                    option.clone(),
                    i,
                    option.environment.topic_salt.clone(),
                )
                .await
            },
            format!("producer-{i}")
        );

        producer_wait.push(producer);
    }

    let _setup_status = fork_and_wait! {
        fluvio_future::task::run_block_on(async {
            let mut test_driver_setup = test_driver.clone();

            // Connect test driver to cluster before starting test
            test_driver_setup.connect().await.expect("Unable to connect to cluster");


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
            let mut num_producers = option.environment.producer;
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

                    // Idea for interactive runs: we can provide live stats here if producers report metrics
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
