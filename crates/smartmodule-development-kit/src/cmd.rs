use clap::Parser;
use anyhow::Result;

use crate::build::BuildCmd;
use crate::generate::GenerateCmd;
use crate::test::TestCmd;
use crate::load::LoadCmd;
use crate::publish::PublishCmd;
use crate::hub::HubCmd;
use crate::set_public::SetPublicOpt;

/// SmartModule Development Kit utility
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Builds SmartModule into WASM
    Build(BuildCmd),
    /// Generates a new SmartModule Project
    Generate(GenerateCmd),
    Test(TestCmd),
    Load(LoadCmd),
    /// Publish SmartModule to Hub
    Publish(PublishCmd),
    /// Hub options
    #[clap(subcommand, hide = true)]
    Hub(HubCmd),
    /// Set package as public
    #[clap(name = "set-public")]
    SetPublic(SetPublicOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            SmdkCommand::Build(opt) => opt.process(),
            SmdkCommand::Generate(opt) => opt.process(),
            SmdkCommand::Test(opt) => opt.process(),
            SmdkCommand::Load(opt) => opt.process(),
            SmdkCommand::Publish(opt) => opt.process(),
            SmdkCommand::Hub(opt) => opt.process(),
            SmdkCommand::SetPublic(opt) => opt.process(),
        }
    }
}
