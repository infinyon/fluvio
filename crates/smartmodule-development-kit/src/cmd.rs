use anyhow::Result;
use cargo_builder::package::PackageOption;
use clap::Parser;
use fluvio_cli_common::version_cmd::BasicVersionCmd;

use crate::build::{BuildCmd, BUILD_TARGET};
use crate::generate::GenerateCmd;
use crate::test::TestCmd;
use crate::load::LoadCmd;
use crate::publish::PublishCmd;
use crate::hub::HubCmd;
use crate::set_public::SetPublicOpt;
use crate::clean::CleanCmd;

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
    #[command(subcommand, hide = true)]
    Hub(HubCmd),
    /// Set package as public
    #[command(name = "set-public")]
    SetPublic(SetPublicOpt),
    /// Print smdk version information
    Version(BasicVersionCmd),
    /// Cleans the current directory
    Clean(CleanCmd),
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
            SmdkCommand::Clean(opt) => opt.process(),
            SmdkCommand::Version(opt) => opt.process("SMDK"),
        }
    }
}

#[derive(Debug, Parser)]
pub(crate) struct PackageCmd {
    /// Release profile name
    #[arg(long, default_value = "release-lto")]
    pub release: String,

    /// Optional package/project name
    #[arg(long, short)]
    pub package_name: Option<String>,
}

impl PackageCmd {
    pub(crate) fn as_opt(&self) -> PackageOption {
        PackageOption {
            release: self.release.clone(),
            package_name: self.package_name.clone(),
            target: BUILD_TARGET.to_string(),
        }
    }
}
