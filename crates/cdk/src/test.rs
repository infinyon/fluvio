use std::{
    fmt::Debug,
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::{Result, anyhow};
use clap::Parser;

use cargo_builder::{package::PackageInfo, cargo::Cargo};

use crate::cmd::PackageCmd;

/// Builds and runs the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct TestCmd {
    #[clap(flatten)]
    package: PackageCmd,

    #[clap(short, long, value_name = "PATH")]
    config: PathBuf,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

impl TestCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt)?;

        let cargo = Cargo::build()
            .profile(opt.release)
            .lib(false)
            .package(p.package_name())
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()?;

        let status = Command::new(p.target_bin_path()?)
            .current_dir(std::env::current_dir()?)
            .arg("--config")
            .arg(self.config)
            .stdin(Stdio::null())
            .status()?;

        match status.code() {
            Some(0) => Ok(()),
            Some(code) => Err(anyhow!("Connector process returned status {}", code)),
            None => Err(anyhow!("Connector terminated by signal")),
        }
    }
}
