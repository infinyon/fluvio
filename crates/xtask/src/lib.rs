use std::path::PathBuf;
use structopt::StructOpt;
use color_eyre::{Result, eyre::eyre};
use duct::cmd;

mod build;
mod docker;
mod install;

use crate::build::BuildOpt;
use crate::install::InstallTargetOpt;
use crate::install::install_target;
use crate::docker::DockerOpt;
use serde_json::Value;

pub const CARGO: &str = env!("CARGO");

#[derive(StructOpt, Debug)]
pub struct Root {
    #[structopt(subcommand)]
    pub cmd: RootCmd,
}

#[derive(StructOpt, Debug)]
pub enum RootCmd {
    Build(BuildOpt),
    BuildImage(DockerOpt),
    Clippy(BuildOpt),
    Test(BuildOpt),
    #[structopt(aliases = &["test-unit", "unit-test", "unit-tests"])]
    TestUnits(BuildOpt),
    #[structopt(aliases = &["test-doc", "doc-test", "doc-tests"])]
    TestDocs(BuildOpt),
    TestClientDocs(BuildOpt),
    #[structopt(aliases = &["integration-test", "integration-tests"])]
    TestIntegration(BuildOpt),
    InstallTarget(InstallTargetOpt),
}

impl RootCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::Build(opt) => {
                opt.build()?;
            }
            Self::BuildImage(opt) => {
                opt.build_image()?;
            }
            Self::Clippy(opt) => {
                opt.clippy()?;
            }
            Self::Test(opt) => {
                opt.test()?;
            }
            Self::TestDocs(opt) => {
                opt.test_docs()?;
            }
            Self::TestClientDocs(opt) => {
                opt.test_client_docs()?;
            }
            Self::TestUnits(opt) => {
                opt.test_units()?;
            }
            Self::TestIntegration(opt) => {
                opt.test_integration()?;
            }
            Self::InstallTarget(opt) => {
                opt.install_target()?;
            }
        }
        Ok(())
    }
}

/// Sets the following environment variables
///
/// ```text
/// # From old Makefile
/// FLUVIO_BUILD_ZIG ?= zig
/// FLUVIO_BUILD_LLD ?= lld
/// CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
/// CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
/// CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
/// CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
/// ```
pub fn set_env() -> Result<()> {
    let set = |key, value: &str| {
        let value = std::env::var(key).unwrap_or_else(|_| value.to_string());
        std::env::set_var(key, value);
    };
    let ws = workspace_directory()?;
    let bs = |name| {
        ws.join(format!("build-scripts/{}", name))
            .to_string_lossy()
            .to_string()
    };

    set("FLUVIO_BUILD_ZIG", "zig");
    set("FLUVIO_BUILD_LLD", "lld");

    let aarch64_cc = bs("aarch64-linux-musl-zig-cc");
    set("CC_aarch64_unknown_linux_musl", &aarch64_cc);

    let x86_64_cc = bs("x86_64-linux-musl-zig-cc");
    set("CC_x86_64_unknown_linux_musl", &x86_64_cc);

    let ld = bs("ld.lld");
    set("CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER", &ld);
    set("CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER", &ld);

    Ok(())
}

pub fn target_directory() -> Result<PathBuf> {
    let metadata = cmd!(CARGO, "metadata", "--format-version=1").read()?;
    let metadata_json = serde_json::from_str::<Value>(&metadata)?;
    let target_directory = metadata_json["target_directory"]
        .as_str()
        .ok_or_else(|| eyre!("failed to read target directory"))?;
    Ok(PathBuf::from(target_directory))
}

pub fn workspace_directory() -> Result<PathBuf> {
    let metadata = cmd!(CARGO, "metadata", "--format-version=1").read()?;
    let metadata_json = serde_json::from_str::<Value>(&metadata)?;
    let workspace_directory = metadata_json["workspace_root"]
        .as_str()
        .ok_or_else(|| eyre!("failed to read target directory"))?;
    Ok(PathBuf::from(workspace_directory))
}

pub fn dockerfile_path() -> Result<PathBuf> {
    let workspace = workspace_directory()?;
    let dockerfile = workspace.join("Dockerfile");
    Ok(dockerfile)
}
