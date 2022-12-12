use cargo_builder::package::PackageOption;
use clap::Parser;
use anyhow::Result;

use crate::build::BuildCmd;
use crate::deploy::DeployCmd;
use crate::publish::PublishCmd;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Build(BuildCmd),
    Deploy(DeployCmd),
    Publish(PublishCmd),
    Test,
}

impl CdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            CdkCommand::Build(opt) => opt.process(),
            CdkCommand::Test => todo!(),
            CdkCommand::Deploy(opt) => opt.process(),
            CdkCommand::Publish(opt) => opt.process(),
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
