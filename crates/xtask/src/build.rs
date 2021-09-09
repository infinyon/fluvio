use std::path::PathBuf;
use structopt::StructOpt;
use color_eyre::Result;
use duct::cmd;

use crate::{CARGO, target_directory};
use crate::install_target;

#[derive(StructOpt, Debug, Default)]
pub struct BuildOpt {
    /// Build in release mode
    #[structopt(long)]
    pub release: bool,
    /// Build in verbose mode
    #[structopt(long)]
    pub verbose: bool,
    /// (Optional) A specific platform target to build for
    #[structopt(long)]
    pub target: Option<String>,
}

impl BuildOpt {
    pub fn build(&self) -> Result<()> {
        println!("Building all artifacts");
        self.build_cli()?;
        self.build_cluster()?;
        self.build_test()?;
        self.build_smartstreams()?;
        Ok(())
    }

    pub fn build_cli(&self) -> Result<PathBuf> {
        install_target(self.target.as_deref())?;
        let path = self.build_bin("fluvio")?;
        Ok(path)
    }

    pub fn build_cluster(&self) -> Result<PathBuf> {
        install_target(self.target.as_deref())?;
        let path = self.build_bin("fluvio-run")?;
        Ok(path)
    }

    pub fn build_test(&self) -> Result<PathBuf> {
        install_target(self.target.as_deref())?;
        let path = self.build_bin("fluvio-test")?;
        Ok(path)
    }

    /// Builds the named binary, returning the path of the produced executable
    pub fn build_bin(&self, bin: &str) -> Result<PathBuf> {
        println!("Building {}", bin);

        let args = vec![
            Some("build".to_string()),
            Some("--bin".to_string()),
            Some(bin.to_string()),
            self.verbose.then(|| "--verbose".to_string()),
            self.release.then(|| "--release".to_string()),
            self.target.as_deref().map(|t| format!("--target={}", t)),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        duct::cmd(CARGO, args).run()?;

        // Assemble the absolute path of the binary we just built
        let mut path = target_directory()?;
        if let Some(target) = self.target.as_ref() {
            path.push(target);
        }
        let profile = if self.release { "release" } else { "debug" };
        path.push(profile);
        path.push(bin);

        Ok(path)
    }

    pub fn build_smartstreams(&self) -> Result<()> {
        install_target(Some("wasm32-unknown-unknown"))?;
        cmd!(
            CARGO,
            "build",
            "--target=wasm32-unknown-unknown",
            "--manifest-path=crates/fluvio-smartstream/examples/Cargo.toml",
        )
        .run()?;
        Ok(())
    }

    pub fn test(&self) -> Result<()> {
        self.test_units()?;
        self.test_docs()?;
        self.test_integration()?;
        Ok(())
    }

    pub fn test_units(&self) -> Result<()> {
        cmd!(CARGO, "test", "--lib", "--all-features").run()?;
        Ok(())
    }

    pub fn test_docs(&self) -> Result<()> {
        cmd!(CARGO, "test", "--doc", "--all-features").run()?;
        Ok(())
    }

    pub fn test_integration(&self) -> Result<()> {
        cmd!(
            CARGO,
            "test",
            "--lib",
            "--all-features",
            "--",
            "--ignored",
            "--test-threads=1"
        )
        .run()?;
        Ok(())
    }

    pub fn clippy(&self) -> Result<()> {
        println!("Checking clippy");
        // Use `cargo check` first to leverage any caching
        cmd!(CARGO, "check", "--all", "--all-features", "--tests").run()?;
        cmd!(
            CARGO,
            "clippy",
            "--all",
            "--all-features",
            "--tests",
            "--",
            "-D",
            "warnings",
            "-A",
            "clippy::upper_case_acronyms"
        )
        .run()?;
        Ok(())
    }
}
