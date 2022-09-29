use clap::Parser;
use anyhow::Result;

use crate::build::BuildOpt;
use crate::generate::GenerateOpt;
use crate::test::TestOpt;

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Generate new SmartModule project
    Generate(GenerateOpt),
    /// Builds SmartModule into WASM
    Build(BuildOpt),
    Test(TestOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            Self::Generate(opt) => opt.process(),
            Self::Build(opt) => opt.process(),
            SmdkCommand::Test(opt) => opt.process(),
        }
    }
}
