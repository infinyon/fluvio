pub mod producer;
pub mod consumer;
pub mod util;

use std::any::Any;
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use async_lock::RwLock;
use structopt::StructOpt;

use fluvio_future::task::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;

use futures::join;

#[derive(Debug, Clone)]
pub struct LongevityTestCase {
    pub environment: EnvironmentSetup,
    pub option: LongevityTestOption,
}

impl From<TestCase> for LongevityTestCase {
    fn from(test_case: TestCase) -> Self {
        let longevity_option = test_case
            .option
            .as_any()
            .downcast_ref::<LongevityTestOption>()
            .expect("LongevityTestOption")
            .to_owned();
        LongevityTestCase {
            environment: test_case.environment,
            option: longevity_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Longevity Test")]
pub struct LongevityTestOption {
    // total time we want the producer to run, in seconds
    #[structopt(long, parse(try_from_str = parse_seconds), default_value = "3600")]
    runtime_seconds: Duration,

    // This should be mutually exclusive with runtime_seconds
    // num_records: u32

    // record payload size used by test, in bytes
    #[structopt(long, default_value = "1000")]
    record_size: usize,

    // TODO: Support these workflows
    //#[structopt(long)]
    //pub disable_producer: bool,
    //#[structopt(long)]
    //pub disable_consumer: bool,

    // Offset the consumer should start from
    //#[structopt(long, default_value = "0")]
    //pub consumer_offset: u32,
    /// Opt-in to detailed output printed to stdout
    #[structopt(long, short)]
    verbose: bool,
}

fn parse_seconds(s: &str) -> Result<Duration, ParseIntError> {
    let seconds = s.parse::<u64>()?;
    Ok(Duration::from_secs(seconds))
}

impl TestOption for LongevityTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "longevity")]
pub async fn longevity(
    mut test_driver: Arc<RwLock<FluvioTestDriver>>,
    mut test_case: TestCase,
) -> TestResult {
    test_longevity_consume_produce(test_driver.clone(), test_case.into()).await
}

pub async fn test_longevity_consume_produce(
    test_driver: Arc<RwLock<TestDriver>>,
    option: LongevityTestCase,
) {
    println!("Testing longevity consumer and producer");

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    let (sender, receiver) = async_channel::unbounded();

    let consumer_join = spawn(consumer::consumer_stream(
        test_driver.clone(),
        option.clone(),
        receiver,
    ));
    let producer_join = producer::producer(test_driver, option, sender);

    join!(consumer_join, producer_join);
}
