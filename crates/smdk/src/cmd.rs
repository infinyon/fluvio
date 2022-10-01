use clap::Parser;
use anyhow::Result;

use crate::build::BuildOpt;
use crate::generate::GenerateOpt;
use crate::test::TestOpt;
use crate::load::LoadOpt;

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Builds SmartModule into WASM
    Build(BuildOpt),
    /// Generates a new SmartModule Project
    Generate(GenerateOpt),
    Test(TestOpt),
    Load(LoadOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            SmdkCommand::Build(opt) => opt.process(),
            SmdkCommand::Generate(opt) => opt.process(),
            SmdkCommand::Test(opt) => opt.process(),
            SmdkCommand::Load(opt) => opt.process(),
        }
    }
}
