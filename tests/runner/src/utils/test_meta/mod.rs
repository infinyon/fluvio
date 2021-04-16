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

#[derive(Debug, Clone)]
pub struct TestResult {
    pub success: bool,
    pub duration: Duration,
}

impl TestResult {
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for TestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let success_str = format!("{}", self.success);
        let duration_str = format!("{:?}", self.duration);

        let table = table!(
            [b->"Test Results"],
            ["Pass?", b->success_str],
            ["Duration", duration_str]
        );

        write!(f, "{}", table)
    }
}
