pub mod derive_attr;
pub mod environment;
pub mod test_result;
pub mod test_timer;
pub mod fork;

use std::any::Any;
use std::fmt::Debug;

use structopt::StructOpt;
use structopt::clap::AppSettings;

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
