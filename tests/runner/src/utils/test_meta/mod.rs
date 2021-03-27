pub mod derive_attr;
pub mod environment;

use std::fmt::{self, Debug, Display, Formatter};

use structopt::StructOpt;
use structopt::clap::AppSettings;
use std::any::Any;
use std::time::{Duration, Instant};

use environment::EnvironmentSetup;

pub trait TestOption: Debug {
    fn as_any(&self) -> &dyn Any;
}

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
    pub test_name: String,

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

#[derive(Debug, Clone)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
    // perf_results?
}

impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let output = vec![
            format!(""),
            "Test Results:".to_string(),
            format!("Pass    : {}", self.success),
            format!("Duration: {:?}", self.duration),
            format!(""),
        ];

        write!(f, "{}", output.join("\n"))
    }
}
