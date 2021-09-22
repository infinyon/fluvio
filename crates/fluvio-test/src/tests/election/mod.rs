use std::any::Any;
use std::sync::Arc;

use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use async_lock::RwLock;

#[derive(Debug, Clone)]
pub struct ElectionTestCase {
    pub environment: EnvironmentSetup,
    pub option: ElectionTestOption,
}

impl From<TestCase> for ElectionTestCase {
    fn from(test_case: TestCase) -> Self {
        let election_option = test_case
            .option
            .as_any()
            .downcast_ref::<ElectionTestOption>()
            .expect("SmokeTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: election_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio ELECTION Test")]
pub struct ElectionTestOption {}

impl TestOption for ElectionTestOption {
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
pub async fn election(
    mut test_driver: Arc<RwLock<FluvioTestDriver>>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting election test");
}
