use clap::Parser;
use anyhow::Result;

use crate::generate::GenerateOpt;
use crate::test::TestOpt;

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Generate new SmartModule project
    Generate(GenerateOpt),
    Test(TestOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            Self::Generate(opt) => opt.process(),
            SmdkCommand::Test(opt) => opt.process(),
        }
    }
}
