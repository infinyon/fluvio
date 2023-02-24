use cargo_builder::package::PackageOption;
use clap::Parser;
use anyhow::Result;

use crate::build::BuildCmd;
use crate::generate::GenerateCmd;
use crate::deploy::DeployCmd;
use crate::test::TestCmd;
use crate::publish::PublishCmd;
use crate::set_public::SetPublicCmd;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Build(BuildCmd),
    Test(TestCmd),
    Generate(GenerateCmd),
    Deploy(DeployCmd),
    Publish(PublishCmd),

    #[clap(name = "set-public")]
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
        }
    }
}

#[derive(Debug, Parser)]
pub struct PackageCmd {
    /// Release profile name
    #[clap(long, default_value = "release")]
    pub release: String,

    /// Optional package/project name
    #[clap(long, short)]
    pub package_name: Option<String>,
}

impl PackageCmd {
    pub(crate) fn as_opt(&self) -> PackageOption {
        PackageOption {
            release: self.release.clone(),
            package_name: self.package_name.clone(),
        }
    }
}
