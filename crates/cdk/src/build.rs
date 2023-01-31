use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;

use cargo_builder::{package::PackageInfo, cargo::Cargo};

use crate::cmd::PackageCmd;

/// Build the Connector in the current working directory
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
            .lib(false)
            .package(p.package_name())
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()
    }
}
