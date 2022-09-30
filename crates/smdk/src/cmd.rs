use clap::Parser;
use anyhow::Result;

use crate::build::BuildOpt;
use crate::test::TestOpt;

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Builds SmartModule into WASM
    Build(BuildOpt),
    Test(TestOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            Self::Build(opt) => opt.process(),
            SmdkCommand::Test(opt) => opt.process(),
        }
    }
}
