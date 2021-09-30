pub mod producer;
pub mod consumer;
pub mod util;

use core::panic;
use std::any::Any;
use std::num::ParseIntError;
use std::time::Duration;
use std::process::exit;
use std::thread;
use structopt::StructOpt;

use fluvio_future::task::run_block_on;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;

use fork::{fork, Fork};
use nix::sys::wait::waitpid;
use nix::unistd::Pid;

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
pub fn longevity(mut test_driver: FluvioTestDriver, mut test_case: TestCase) -> TestResult {
    let option: LongevityTestCase = test_case.into();

    println!("Testing longevity consumer and producer");

    if !option.option.verbose {
        println!("Run with `--verbose` flag for more test output");
    }

    let consumer_process = match fork() {
        Ok(Fork::Parent(child_pid)) => child_pid,
        Ok(Fork::Child) => {
            run_block_on(async {
                println!("Consumer about to connect");
                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");
                println!("About to start consumer test");
                consumer::consumer_stream(test_driver.clone(), option.clone()).await
            });

            exit(0);
        }
        Err(_) => panic!("Consumer fork failed"),
    };

    //let consumer_join = spawn(consumer::consumer_stream(
    //    test_driver.clone(),
    //    option.clone(),
    //));
    let consumer_wait = thread::spawn(move || {
        let pid = Pid::from_raw(consumer_process);
        match waitpid(pid, None) {
            Ok(status) => {
                println!("[main] Producer Child exited with status {:?}", status);
            }
            Err(err) => panic!("[main] waitpid() failed: {}", err),
        }
    });

    let producer_process = match fork() {
        Ok(Fork::Parent(child_pid)) => child_pid,
        Ok(Fork::Child) => {
            run_block_on(async {
                println!("Producer about to connect");
                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");
                println!("About to start producer test");
                producer::producer(test_driver, option).await
            });

            exit(0);
        }
        Err(_) => panic!("Producer fork failed"),
    };

    let producer_wait = thread::spawn(move || {
        let pid = Pid::from_raw(producer_process);
        match waitpid(pid, None) {
            Ok(status) => {
                println!("[main] Producer Child exited with status {:?}", status);
            }
            Err(err) => panic!("[main] waitpid() failed: {}", err),
        }
    });

    //let producer_join = producer::producer(test_driver, option);

    let _ = producer_wait.join();
    let _ = consumer_wait.join();
    //join!(consumer_wait, producer_wait);
}
