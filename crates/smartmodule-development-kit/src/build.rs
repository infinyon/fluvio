use std::fmt::Debug;
use std::str;

use anyhow::Result;
use clap::Parser;

use cargo_builder::package::PackageInfo;
use cargo_builder::cargo::Cargo;

use crate::cmd::PackageCmd;

pub(crate) const BUILD_TARGET: &str = "wasm32-unknown-unknown";

/// Builds the SmartModule in the current working directory into a WASM file
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
        let p = PackageInfo::from_options(&opt)?;

        let cargo = Cargo::build()
            .profile(opt.release)
            .lib(true)
            .package(p.package_name())
            .target(BUILD_TARGET)
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()
    }
}
