use std::process::{Command, Stdio};
use std::fmt::Debug;
use std::str;

use anyhow::{Error, Result, anyhow};
use clap::Parser;
use crate::package::{PackageInfo, PackageOption};

pub(crate) const BUILD_TARGET: &str = "wasm32-unknown-unknown";

/// Builds the SmartModule in the current working directory into a WASM file
#[derive(Debug, Parser)]
pub struct BuildOpt {
    #[clap(flatten)]
    package: PackageOption,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

impl BuildOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let p = PackageInfo::from_options(&self.package).map_err(|e| anyhow::anyhow!(e))?;

        let mut cargo = BuildOpt::make_cargo_cmd()?;

        let cwd = std::env::current_dir()?;
        cargo
            .current_dir(&cwd)
            .arg("build")
            .arg("--profile")
            .arg(&self.package.release)
            .arg("--lib")
            .arg("-p")
            .arg(p.package);
        cargo.arg("--target").arg(BUILD_TARGET);
        if !self.extra_arguments.is_empty() {
            cargo.args(&self.extra_arguments);
        }
        let status = cargo.status().map_err(Error::from)?;

        if status.success() {
            Ok(())
        } else {
            let output = cargo.output()?;
            let stderr = String::from_utf8(output.stderr)?;
            Err(anyhow!(stderr))
        }
    }

    fn make_cargo_cmd() -> Result<Command> {
        let mut cargo = Command::new("cargo");

        cargo.output().map_err(Error::from)?;
        cargo.stdout(Stdio::inherit());
        cargo.stderr(Stdio::inherit());

        Ok(cargo)
    }
}
