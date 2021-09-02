use structopt::StructOpt;
use color_eyre::Result;

mod build;
mod install;

use crate::build::BuildOpt;
use crate::install::InstallTargetOpt;
use crate::install::install_target;

pub const CARGO: &str = env!("CARGO");

#[derive(StructOpt, Debug)]
pub struct Root {
    #[structopt(subcommand)]
    pub cmd: RootCmd,
}

#[derive(StructOpt, Debug)]
pub enum RootCmd {
    Build(BuildOpt),
    Clippy(BuildOpt),
    Test(BuildOpt),
    #[structopt(aliases = &["test-unit", "unit-test", "unit-tests"])]
    TestUnits(BuildOpt),
    #[structopt(aliases = &["test-doc", "doc-test", "doc-tests"])]
    TestDocs(BuildOpt),
    TestClientDocs(BuildOpt),
    #[structopt(aliases = &["integration-test", "integration-tests"])]
    TestIntegration(BuildOpt),
    InstallTarget(InstallTargetOpt),
}

impl RootCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::Build(opt) => {
                opt.build()?;
            }
            Self::Clippy(opt) => {
                opt.clippy()?;
            }
            Self::Test(opt) => {
                opt.test()?;
            }
            Self::TestDocs(opt) => {
                opt.test_docs()?;
            }
            Self::TestClientDocs(opt) => {
                opt.test_client_docs()?;
            }
            Self::TestUnits(opt) => {
                opt.test_units()?;
            }
            Self::TestIntegration(opt) => {
                opt.test_integration()?;
            }
            Self::InstallTarget(opt) => {
                opt.install_target()?;
            }
        }
        Ok(())
    }
}
