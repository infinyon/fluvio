pub mod derive_attr;
pub mod environment;

use std::any::Any;
use std::time::{Duration, Instant};
use std::fmt::{self, Debug, Display, Formatter};

use structopt::StructOpt;
use structopt::clap::AppSettings;
use prettytable::{table, row, cell};

use environment::EnvironmentSetup;

use dyn_clone::DynClone;

pub trait TestOption: Debug + DynClone {
    fn as_any(&self) -> &dyn Any;
}

dyn_clone::clone_trait_object!(TestOption);

#[derive(Debug, Clone)]
pub struct TestCase {
    pub environment: EnvironmentSetup,
    pub option: Box<dyn TestOption>,
}

impl TestCase {
    pub fn new(environment: EnvironmentSetup, option: Box<dyn TestOption>) -> Self {
        Self {
            environment,
            option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, PartialEq)]
pub enum TestCli {
    #[structopt(external_subcommand)]
    Args(Vec<String>),
}

impl Default for TestCli {
    fn default() -> Self {
        TestCli::Args(Vec::new())
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(
    name = "fluvio-test-runner",
    about = "Test fluvio platform",
    global_settings = &[AppSettings::ColoredHelp])]
pub struct BaseCli {
    #[structopt(flatten)]
    pub environment: EnvironmentSetup,

    #[structopt(subcommand)]
    pub test_cmd_args: Option<TestCli>,
}

#[derive(Debug, Clone)]
pub struct TestTimer {
    pub start_time: Instant,
    pub duration: Option<Duration>,
}

impl TestTimer {
    pub fn start() -> Self {
        TestTimer {
            start_time: Instant::now(),
            duration: None,
        }
    }

    pub fn stop(&mut self) {
        self.duration = Some(self.start_time.elapsed());
    }

    pub fn duration(&self) -> Duration {
        self.duration.expect("Timer is still running")
    }
}

#[derive(Debug, Clone, Default)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
    // stats
    pub bytes_produced: u64,
    pub produce_latency: u64,
    pub num_producers: u64,
    pub bytes_consumed: u64,
    pub consume_latency: u64,
    pub num_consumers: u64,
    pub num_topics: u64,
    pub topic_create_latency: u64,
}

impl TestResult {
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

// TODO: Parse the time scalars into Duration
impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let success_str = format!("{}", self.success);
        let duration_str = format!("{:?}", self.duration);
        let producer_latency_str = format!("{:?}", Duration::from_nanos(self.produce_latency));
        let consumer_latency_str = format!("{:?}", Duration::from_nanos(self.consume_latency));
        let topic_create_latency_str =
            format!("{:?}", Duration::from_nanos(self.topic_create_latency));

        let table = table!(
            [b->"Test Results"],
            ["Pass?", b->success_str],
            ["Duration", duration_str],
            //

            ["# topics created", self.num_topics],
            ["topic create latency 99.9%", topic_create_latency_str],
            ["# producers created", self.num_producers],
            ["bytes produced", self.bytes_produced],
            ["producer latency 99.9%", producer_latency_str],
            ["# consumers created", self.num_consumers],
            ["bytes consumed", self.bytes_consumed],
            ["consumer latency 99.9%", consumer_latency_str]
        );

        write!(f, "{}", table)
    }
}
