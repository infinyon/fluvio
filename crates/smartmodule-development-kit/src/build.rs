use std::fmt::Debug;
use std::str;

use anyhow::Result;
use clap::Parser;

use cargo_builder::package::PackageInfo;
use cargo_builder::cargo::Cargo;

use crate::cmd::PackageCmd;
use crate::ENV_SMDK_WASI;

pub(crate) const BUILD_TARGET: &str = "wasm32-unknown-unknown";
pub(crate) const BUILD_TARGET_WASI: &str = "wasm32-wasi";

/// Builds the SmartModule in the current working directory into a WASM file
#[derive(Debug, Parser)]
pub struct BuildCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,

    /// Build wasi target
    #[arg(long, env = ENV_SMDK_WASI)]
    wasi: bool,
}

impl BuildCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt)?;

        let build_target = if self.wasi {
            BUILD_TARGET_WASI
        } else {
            BUILD_TARGET
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
