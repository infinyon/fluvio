use std::sync::Arc;
use std::any::Any;
use structopt::StructOpt;

use fluvio::{Fluvio, TopicProducer};
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase, TestResult};
use fluvio_test_util::test_runner::FluvioTest;

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

async fn get_producer(client: Arc<Fluvio>, topic_name: String) -> TopicProducer {
    client
        .topic_producer(topic_name.clone())
        .await
        .expect("Couldn't get producer")
}

#[fluvio_test(name = "producer_stress", topic = "test", benchmark = true)]
pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) -> TestResult {
    let test_case: ProducerStressTestCase = test_case.into();

    if !test_case.environment.is_benchmark() {
        println!("\nStarting single-process producer stress");

        println!(
            "Producers              : {}",
            test_case.option.producers.clone()
        );
        println!(
            "# messages per producer: {}",
            test_case.option.iteration.clone()
        );
    }

    let long_str = String::from("aaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeefffffffffffffffffffffffffffggggggggggggggggg");
    let topic_name = test_case.environment.topic_name;

    let mut producers = Vec::new();
    for _ in 0..test_case.option.producers {
        let producer = get_producer(client.clone(), topic_name.clone()).await;
        producers.push(producer);
    }

    for n in 1..test_case.option.iteration + 1 {
        for (i, p) in producers.iter().enumerate() {
            let message = format!("producer-{} line-{} {}", i, n, long_str.clone());

            p.send_record(message.clone(), 0)
                .await
                .unwrap_or_else(|_| panic!("send record failed for iteration: {}", n));
        }
    }

    // Make the compiler happy
    drop(producers);
    drop(client);
}
