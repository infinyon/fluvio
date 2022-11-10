use clap::Parser;
use anyhow::Result;

use crate::build::BuildOpt;
use crate::generate::GenerateOpt;
use crate::test::TestOpt;
use crate::load::LoadOpt;
use crate::publish::PublishOpt;
use crate::hub::HubCmd;
use crate::set_public::SetPublicOpt;

/// SmartModule Development Kit utility
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Builds SmartModule into WASM
    Build(BuildOpt),
    /// Generates a new SmartModule Project
    Generate(GenerateOpt),
    Test(TestOpt),
    Load(LoadOpt),
    /// Publish SmartModule to Hub
    Publish(PublishOpt),
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
