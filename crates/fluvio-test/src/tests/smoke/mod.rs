pub mod consume;
pub mod produce;
pub mod message;
pub mod offsets;

use std::any::Any;

use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

#[derive(Debug, Clone)]
pub struct SmokeTestCase {
    pub environment: EnvironmentSetup,
    pub option: SmokeTestOption,
}

impl From<TestCase> for SmokeTestCase {
    fn from(test_case: TestCase) -> Self {
        let smoke_option = test_case
            .option
            .as_any()
            .downcast_ref::<SmokeTestOption>()
            .expect("SmokeTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: smoke_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Smoke Test")]
pub struct SmokeTestOption {
    #[structopt(long)]
    pub use_cli: bool,
    #[structopt(long, default_value = "1")]
    pub producer_iteration: u32,
    #[structopt(long, default_value = "100")]
    pub producer_record_size: u32,
    #[structopt(long)]
    pub consumer_wait: bool,
}

impl TestOption for SmokeTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

//inventory::submit! {
//    FluvioTest {
//        name: "smoke".to_string(),
//        test_fn: smoke,
//        validate_fn: validate_subcommand,
//    }
//}

//pub fn validate_subcommand(subcmd: Vec<String>) -> Box<dyn TestOption> {
//    Box::new(SmokeTestOption::from_iter(subcmd))
//}

#[fluvio_test(topic = "test")]
pub fn smoke(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let smoke_test_case = test_case.into();

    // We're going to handle the `--consumer-wait` flag in this process
    let producer_wait = async_process!(async {
        let mut test_driver_consumer_wait = test_driver.clone();

        test_driver
            .connect()
            .await
            .expect("Connecting to cluster failed");
        println!("About to start producer test");

        let start_offset = produce::produce_message(test_driver, &smoke_test_case).await;

        // If we've passed in `--consumer-wait` then we should start the consumer after the producer
        if smoke_test_case.option.consumer_wait {
            test_driver_consumer_wait
                .connect()
                .await
                .expect("Connecting to cluster failed");
            use crate::tests::smoke::consume::validate_consume_message_api;
            validate_consume_message_api(test_driver_consumer_wait, start_offset, &smoke_test_case)
                .await;
        }
    });

    // By default, we should run the consumer and producer at the same time
    if !smoke_test_case.option.consumer_wait {
        let consumer_wait = async_process!(async {
            test_driver
                .connect()
                .await
                .expect("Connecting to cluster failed");
            consume::validate_consume_message(test_driver, &smoke_test_case).await;
        });

        let _ = consumer_wait.join();
    }
    let _ = producer_wait.join();
}
