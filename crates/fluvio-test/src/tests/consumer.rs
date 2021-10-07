use std::any::Any;
//use std::env;
//use dataplane::record::RecordSet;
use structopt::StructOpt;

//use fluvio::RecordKey;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
//use fluvio_future::task::run_block_on;

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
    #[structopt(long, default_value = "3")]
    pub consumers: u16,

    // We basically need to know when to stop
    #[structopt(long, default_value = "3")]
    pub num_records: u16, // duration
                          // -1 is no stopping
                          // We're going to need to eventually catch signal to print output

                          // starting offset
                          // 0 is beginning, -1 is end

                          // These need to be mutually exclusive
                          // partition #
                          // multi-partition
}

impl TestOption for ConsumerTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Default to using the producer test's topic
#[fluvio_test(name = "consumer", topic = "producer-test")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let test_case: ConsumerTestCase = test_case.into();

    println!("\nStarting Consumer test");

    println!(
        "Consumers              : {}",
        test_case.option.consumers.clone()
    );

    //let long_str = String::from("aaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeefffffffffffffffffffffffffffggggggggggggggggg");
    //let topic_name = test_case.environment.topic_name();

    //let mut producers = Vec::new();
    //for _ in 0..test_case.option.producers {
    //    let producer = test_driver.create_producer(&topic_name).await;
    //    producers.push(producer);
    //}

    //for n in 1..test_case.option.iteration + 1 {
    //    for (i, p) in producers.iter().enumerate() {
    //        let message = format!("producer-{} line-{} {}", i, n, long_str.clone())
    //            .as_bytes()
    //            .to_vec();

    //        // This is for CI stability. We need to not panic during CI, but keep errors visible
    //        if let Ok(is_ci) = env::var("CI") {
    //            if is_ci == "true" {
    //                test_driver
    //                    .send_count(p, RecordKey::NULL, message)
    //                    .await
    //                    .unwrap_or_else(|_| {
    //                        eprintln!(
    //                            "[CI MODE] send record failed for iteration: {} message: {}",
    //                            n, i
    //                        );
    //                    });
    //            }
    //        } else {
    //            test_driver
    //                .send_count(p, RecordKey::NULL, message)
    //                .await
    //                .unwrap_or_else(|_| {
    //                    panic!("send record failed for iteration: {} message: {}", n, i)
    //                });
    //        }
    //    }
    //}

    ////let lock = test_driver.read().await;
    ////println!(
    ////    "Producer latency 99%: {:?}",
    ////    lock.producer_latency_histogram.value_at_quantile(0.99)
    ////);
    ////drop(lock);
}
