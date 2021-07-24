pub mod consume;
pub mod produce;
pub mod message;

use std::any::Any;
use std::sync::Arc;
use structopt::StructOpt;

use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use async_lock::RwLock;

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
#[structopt(name = "Fluvio Smoke Test")]
pub struct SmokeTestOption {
    #[structopt(long)]
    pub use_cli: bool,
    #[structopt(long, default_value = "1")]
    pub producer_iteration: u16,
    // Deprecating this flag in favor of TestDriver flag `message-size`
    #[structopt(long, default_value = "1000")]
    pub producer_record_size: u16,
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
pub async fn smoke(
    mut test_driver: Arc<RwLock<TestDriver>>,
    mut test_case: TestCase,
) -> TestResult {
    let mut smoke_test_case: SmokeTestCase = test_case.into();

    if smoke_test_case.option.producer_record_size != 1000
        && smoke_test_case.environment.message_size
            != smoke_test_case.option.producer_record_size as usize
    {
        eprintln!("--producer-record-size is deprecated. In the future use flv-test option `--message-size`");
        smoke_test_case.option.producer_record_size =
            smoke_test_case.environment.message_size as u16;
    }

    let start_offsets = produce::produce_message(test_driver.clone(), &smoke_test_case).await;
    consume::validate_consume_message(test_driver.clone(), &smoke_test_case, start_offsets).await;
}
