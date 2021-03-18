//pub mod consume;
//pub mod produce;
//pub mod message;

use std::collections::HashMap;
use std::str::FromStr;

use crate::test_meta::{TestCase, EnvironmentSetup, TestCli};
use structopt::StructOpt;

#[derive(Debug, Clone)]
pub struct SmokeTestCase {
    pub environment: EnvironmentSetup,
    pub name: String,
    pub vars: TestCli,
}

impl From<TestCase> for SmokeTestCase {
    fn from(test_case: TestCase) -> Self {
        SmokeTestCase {
            environment: test_case.environment,
            name: test_case.name,
            vars: TestCli::default(),
        }
    }
}
#[derive(Debug, Clone, StructOpt)]
pub struct SmokeTestOption {
    #[structopt(long)]
    use_cli: bool,
    #[structopt(long, default_value = "1")]
    producer_iteration: u16,
    #[structopt(long, default_value = "100")]
    producer_record_size: u16,
    #[structopt(long)]
    consumer_wait: bool,
}

impl Default for SmokeTestOption {
    fn default() -> Self {
        SmokeTestOption {
            use_cli: false,
            producer_iteration: 1,
            producer_record_size: 100,
            consumer_wait: false,
        }
    }
}

impl From<HashMap<String, String>> for SmokeTestOption {
    fn from(hashmap: HashMap<String, String>) -> Self {
        let mut option = SmokeTestOption::default();

        if let Some(use_cli) = hashmap.get("use_cli") {
            option.use_cli = bool::from_str(use_cli).expect("bool");
        }

        if let Some(iteration) = hashmap.get("producer.iteration") {
            option.producer_iteration = u16::from_str_radix(iteration, 10).expect("u16");
        }

        if let Some(record_size) = hashmap.get("producer.record_size") {
            option.producer_record_size = u16::from_str_radix(record_size, 10).expect("u16");
        }

        if let Some(consumer_wait) = hashmap.get("consumer.wait") {
            option.consumer_wait = bool::from_str(consumer_wait).expect("bool");
        }

        option
    }
}
