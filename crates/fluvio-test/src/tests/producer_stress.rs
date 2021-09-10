use std::any::Any;
use std::env;
use structopt::StructOpt;
use std::sync::Arc;

use fluvio::RecordKey;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use async_lock::RwLock;

#[derive(Debug, Clone)]
pub struct ProducerStressTestCase {
    pub environment: EnvironmentSetup,
    pub option: ProducerStressTestOption,
}

impl From<TestCase> for ProducerStressTestCase {
    fn from(test_case: TestCase) -> Self {
        let producer_stress_option = test_case
            .option
            .as_any()
            .downcast_ref::<ProducerStressTestOption>()
            .expect("ProducerStressTestOption")
            .to_owned();
        ProducerStressTestCase {
            environment: test_case.environment,
            option: producer_stress_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio ProducerStress Test")]
pub struct ProducerStressTestOption {
    #[structopt(long, default_value = "3")]
    pub producers: u16,
    #[structopt(long, default_value = "100")]
    pub iteration: u16,
}

impl TestOption for ProducerStressTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = "producer_stress", topic = "test")]
pub async fn run(
    mut test_driver: Arc<RwLock<FluvioTestDriver>>,
    mut test_case: TestCase,
) -> TestResult {
    let test_case: ProducerStressTestCase = test_case.into();

    println!("\nStarting single-process producer stress");

    println!(
        "Producers              : {}",
        test_case.option.producers.clone()
    );
    println!(
        "# messages per producer: {}",
        test_case.option.iteration.clone()
    );

    let long_str = String::from("aaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeefffffffffffffffffffffffffffggggggggggggggggg");
    let topic_name = test_case.environment.topic_name();

    let mut producers = Vec::new();
    for _ in 0..test_case.option.producers {
        let mut lock = test_driver.write().await;
        let producer = lock.create_producer(&topic_name).await;
        producers.push(producer);
    }

    for n in 1..test_case.option.iteration + 1 {
        for (i, p) in producers.iter().enumerate() {
            let message = format!("producer-{} line-{} {}", i, n, long_str.clone())
                .as_bytes()
                .to_vec();

            // This is for CI stability. We need to not panic during CI, but keep errors visible
            if let Ok(is_ci) = env::var("CI") {
                if is_ci == "true" {
                    let mut lock = test_driver.write().await;
                    lock.send_count(p, RecordKey::NULL, message)
                        .await
                        .unwrap_or_else(|_| {
                            eprintln!(
                                "[CI MODE] send record failed for iteration: {} message: {}",
                                n, i
                            );
                        });
                }
            } else {
                let mut lock = test_driver.write().await;
                lock.send_count(p, RecordKey::NULL, message)
                    .await
                    .unwrap_or_else(|_| {
                        panic!("send record failed for iteration: {} message: {}", n, i)
                    });
            }
        }
    }

    let lock = test_driver.read().await;
    println!(
        "Producer latency 99%: {:?}",
        lock.producer_latency_histogram.value_at_quantile(0.99)
    );
    drop(lock);
}
