use anyhow::{Error, Result};
use clap::Parser;
use std::process::Command;
use std::fmt::Debug;

/// Builds the SmartModule in the current working directory into a WASM file
#[derive(Debug, Parser)]
pub struct BuildOpt;

impl BuildOpt {
    pub(crate) fn process(&self) -> Result<()> {
        BuildOpt::check_cargo_exists()?;

        let mut cmd = Command::new("cargo");
        let cwd = std::env::current_dir()?;

        cmd.current_dir(&cwd)
            .arg("build")
            .arg("--profile")
            .arg("release-lto")
            .arg("--lib");
        cmd.arg("--target").arg("wasm32-unknown-unknown");

        let status = cmd.status()?;

        if status.success() {
            return Ok(());
        }

        Err(Error::msg(
            "An error ocurred building SmartModule into WASM",
        ))
    }

    fn check_cargo_exists() -> Result<()> {
        Command::new("cargo").arg("version").status().map_err(|e| {
            Error::msg(format!(
                "An error ocurrend checking `cargo version` installed. {}",
                e
            ))
        })?;

        Ok(())
    }
}
