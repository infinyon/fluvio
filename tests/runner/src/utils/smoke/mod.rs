pub mod consume;
pub mod produce;
pub mod message;

use std::any::Any;
use crate::test_meta::{EnvironmentSetup, TestOption, TestCase};
use structopt::StructOpt;

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
        SmokeTestCase {
            environment: test_case.environment,
            option: smoke_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
pub struct SmokeTestOption {
    #[structopt(long)]
    pub use_cli: bool,
    #[structopt(long, default_value = "1")]
    pub producer_iteration: u16,
    #[structopt(long, default_value = "100")]
    pub producer_record_size: u16,
    #[structopt(long)]
    pub consumer_wait: bool,
}

impl TestOption for SmokeTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
