use std::any::Any;
use std::env;
use fluvio_test_util::async_process;
use tracing::{Instrument, debug_span};

use clap::Parser;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::{EnvironmentSetup};
use fluvio_test_util::test_meta::{TestOption, TestCase};

#[derive(Debug, Clone)]
pub struct SelfCheckTestCase {
    pub environment: EnvironmentSetup,
    pub option: SelfCheckTestOption,
}

impl From<TestCase> for SelfCheckTestCase {
    fn from(test_case: TestCase) -> Self {
        let self_check_option = test_case
            .option
            .as_any()
            .downcast_ref::<SelfCheckTestOption>()
            .expect("SelfCheckTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: self_check_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Test Self Check")]
pub struct SelfCheckTestOption {
    /// Intentionally panic to test panic handling
    #[clap(long)]
    pub force_panic: bool,
}

impl TestOption for SelfCheckTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test()]
pub fn self_check(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let self_test_case: SelfCheckTestCase = test_case.into();

    // If the CI env var is exists, we're in CI
    if env::var("CI").is_ok() {
        println!("Running in CI")
    }

    println!("Starting Fluvio Test Self-Check");

    let another_process = async_process!(
        async {
            // Sleep for a moment to help (visually) validate global test timer
            std::thread::sleep(std::time::Duration::from_secs(3));

            if self_test_case.option.force_panic {
                panic!("Intentionally panicking inside another process");
            }
        }
        .instrument(debug_span!("another_process")),
        "sleep"
    );

    another_process.join().unwrap();
}
