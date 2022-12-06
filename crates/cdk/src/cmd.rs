use clap::Parser;
use anyhow::Result;

use crate::build::BuildCmd;
use crate::deploy::Deploy;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Build(BuildCmd),
    Deploy(Deploy),
    Test,
}

impl CdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            CdkCommand::Build(opt) => opt.process(),
            CdkCommand::Test => todo!(),
            CdkCommand::Deploy(opt) => opt.process(),
        }
    }
}
