use structopt::StructOpt;
use color_eyre::Result;
use duct::cmd;

use crate::CARGO;
use crate::install_target;

#[derive(StructOpt, Debug, Default)]
pub struct BuildOpt {
    #[structopt(long)]
    release: bool,
    #[structopt(long)]
    verbose: bool,
    target: Option<String>,
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

    pub fn build_cli(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio");
        cmd!(CARGO, "build", "--bin", "fluvio").run()?;
        Ok(())
    }

    pub fn build_cluster(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio-run");
        cmd!(CARGO, "build", "--bin", "fluvio-run").run()?;
        Ok(())
    }

    pub fn build_test(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio-test");
        cmd!(CARGO, "build", "--bin", "fluvio-test").run()?;
        Ok(())
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

    pub fn test_client_docs(&self) -> Result<()> {
        cmd!(
            CARGO,
            "test",
            "--doc",
            "--all-features",
            "--package=fluvio",
            "--package=fluvio-cli",
            "--package=fluvio-cluster"
        )
        .run()?;
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
