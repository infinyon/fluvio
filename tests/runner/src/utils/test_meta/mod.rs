pub mod derive_attr;
pub mod environment;
pub mod test_result;
pub mod test_timer;
pub mod chart_builder;
pub mod data_export;

use std::any::Any;
use std::fmt::Debug;
use std::path::PathBuf;

use structopt::StructOpt;
use structopt::clap::AppSettings;

use environment::EnvironmentSetup;

use dyn_clone::DynClone;

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
pub struct TestRunnerOptions {
    /// The directory path where charts and data will be saved
    #[structopt(long, parse(from_os_str), default_value = "./tests/benchmark-results")]
    pub results_dir: PathBuf,

    /// Print test summary only. Don't save charts and data at the end of the test
    #[structopt(long)]
    pub skip_data_save: bool,
    //#[structopt(long, default_value = ",")]
    //pub csv_delimiter: String,
    /// For benchmarking purposes. If set other than "fluvio", other cluster must be running and configured
    #[structopt(long, default_value="fluvio", possible_values=&["fluvio","pulsar","kafka"])]
    pub cluster_type: String,
}

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
    pub runner_opts: TestRunnerOptions,
    #[structopt(flatten)]
    pub environment: EnvironmentSetup,

    #[structopt(subcommand)]
    pub test_cmd_args: Option<TestCli>,
}
