use std::fmt::Debug;

use anyhow::{Result};
use clap::Parser;

use cargo_builder::package::{PackageInfo, PackageOption};
use cargo_builder::cargo::Cargo;

/// Builds the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct BuildCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

impl BuildCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt).map_err(|e| anyhow::anyhow!(e))?;

        let cargo = Cargo::build()
            .profile(opt.release)
            .lib(false)
            .package(p.package)
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()
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
