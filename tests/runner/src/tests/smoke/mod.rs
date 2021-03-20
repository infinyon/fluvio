pub mod consume;
pub mod produce;
pub mod message;

use std::sync::Arc;
use std::any::Any;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::{EnvironmentSetup, TestOption, TestCase};

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

#[fluvio_test(topic = "test")]
pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) {
    let smoke_test_case = test_case.into();

    let start_offsets = produce::produce_message(client.clone(), &smoke_test_case).await;
    consume::validate_consume_message(client, &smoke_test_case, start_offsets).await;
}
