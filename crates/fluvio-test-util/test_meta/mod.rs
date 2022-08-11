pub mod derive_attr;
pub mod environment;
pub mod test_result;
pub mod test_timer;
pub mod fork;

use std::any::Any;
use std::fmt::Debug;

use clap::Parser;

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

#[derive(Debug, Clone, Parser, Eq, PartialEq)]
pub enum TestCli {
    #[clap(external_subcommand)]
    Args(Vec<String>),
}

impl Default for TestCli {
    fn default() -> Self {
        TestCli::Args(Vec::new())
    }
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
#[clap(
    name = "fluvio-test-runner",
    about = "Test fluvio platform",
    // AppSettings::ColoredHelp is on by default.
)]
pub struct BaseCli {
    #[clap(flatten)]
    pub environment: EnvironmentSetup,

    #[clap(subcommand)]
    pub test_cmd_args: Option<TestCli>,
}
