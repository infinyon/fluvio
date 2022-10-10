use std::process::{Command, Stdio};
use std::fmt::Debug;

use anyhow::{Error, Result};
use clap::Parser;

const DEFAULT_RELEASE_PROFILE: &str = "release-lto";
const BUILD_TARGET: &str = "wasm32-unknown-unknown";

/// Builds the SmartModule in the current working directory into a WASM file
#[derive(Debug, Parser)]
pub struct BuildOpt {
    /// Build release profile
    #[clap(long, default_value = DEFAULT_RELEASE_PROFILE)]
    release: String,
}

impl BuildOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let mut cargo = BuildOpt::make_cargo_cmd()?;
        let cwd = std::env::current_dir()?;

        cargo
            .current_dir(&cwd)
            .arg("build")
            .arg("--profile")
            .arg(self.release.as_str())
            .arg("--lib");
        cargo.arg("--target").arg(BUILD_TARGET);
        cargo.status().map_err(Error::from)?;

        Ok(())
    }

    fn make_cargo_cmd() -> Result<Command> {
        let mut cargo = Command::new("cargo");

        cargo.output().map_err(Error::from)?;
        cargo.stdout(Stdio::inherit());
        cargo.stderr(Stdio::inherit());

        Ok(cargo)
    }
}
