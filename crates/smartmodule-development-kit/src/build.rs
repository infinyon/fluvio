use std::fmt::Debug;
use std::str;

use anyhow::Result;
use clap::Parser;

use cargo_builder::package::PackageInfo;
use cargo_builder::cargo::Cargo;

use crate::cmd::PackageCmd;
use crate::ENV_SMDK_NOWASI;

pub(crate) const BUILD_TARGET: &str = "wasm32-unknown-unknown";
pub(crate) const BUILD_TARGET_WASI: &str = "wasm32-wasip1";

/// Builds the SmartModule in the current working directory into a WASM file
#[derive(Debug, Parser)]
pub struct BuildCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,

    /// Build a non wasi target (only use if needed for backward compatiblity)
    #[arg(long, env = ENV_SMDK_NOWASI, hide_short_help = true)]
    nowasi: bool,
}

impl BuildCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt)?;

        let build_target = if self.nowasi {
            BUILD_TARGET
        } else {
            BUILD_TARGET_WASI
        };
        let cargo = Cargo::build()
            .profile(opt.release)
            .lib(true)
            .package(p.package_name())
            .target(build_target)
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()
    }
}
