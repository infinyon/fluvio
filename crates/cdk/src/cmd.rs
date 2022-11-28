use clap::Parser;
use anyhow::Result;

use crate::build::BuildCmd;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Build(BuildCmd),
    Test,
}

impl CdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            CdkCommand::Build(opt) => opt.process(),
            CdkCommand::Test => todo!(),
        }
    }
}
