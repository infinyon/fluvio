use anyhow::Result;
use cargo_builder::package::PackageOption;
use clap::Parser;
use fluvio_cli_common::version_cmd::BasicVersionCmd;

use crate::build::BuildCmd;
use crate::generate::GenerateCmd;
use crate::deploy::DeployCmd;
use crate::test::TestCmd;
use crate::publish::PublishCmd;
use crate::set_public::SetPublicCmd;
use crate::hub::HubCmd;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Build(BuildCmd),
    Test(TestCmd),
    Generate(GenerateCmd),
    Deploy(DeployCmd),
    Publish(PublishCmd),
    Version(BasicVersionCmd),
    #[command(subcommand)]
    Hub(HubCmd),

    #[command(name = "set-public")]
    SetPublic(SetPublicCmd),
}

impl CdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            CdkCommand::Build(opt) => opt.process(),
            CdkCommand::Test(opt) => opt.process(),
            CdkCommand::Deploy(opt) => opt.process(),
            CdkCommand::Publish(opt) => opt.process(),
            CdkCommand::SetPublic(opt) => opt.process(),
            CdkCommand::Generate(opt) => opt.process(),
            CdkCommand::Version(opt) => opt.process("CDK"),
            CdkCommand::Hub(opt) => opt.process(),
        }
    }
}

#[derive(Debug, Parser)]
pub struct PackageCmd {
    /// Release profile name
    #[arg(long, default_value = "release")]
    pub release: String,

    /// Provide target platform for the package. Optional.
    /// By default the host's one is used.
    #[arg(
        long,
        default_value_t = current_platform::CURRENT_PLATFORM.to_string()
    )]
    pub target: String,

    /// Optional package/project name
    #[arg(long, short)]
    pub package_name: Option<String>,
}

impl PackageCmd {
    pub(crate) fn as_opt(&self) -> PackageOption {
        PackageOption {
            release: self.release.clone(),
            package_name: self.package_name.clone(),
            target: self.target.clone(),
        }
    }
}
