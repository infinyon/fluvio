use std::{fmt::Debug, path::PathBuf};

use anyhow::{Result, Context};
use clap::Parser;

use cargo_builder::{package::PackageInfo, cargo::Cargo};
use fluvio_connector_deployer::{Deployment, DeploymentType};

use crate::{cmd::PackageCmd, deploy::from_cargo_package};

/// Build and run the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct TestCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Path to configuration file in YAML format
    #[clap(short, long, value_name = "PATH", default_value = "sample-config.yaml")]
    config: PathBuf,

    /// Path to file with secrets. Secrets are 'key=value' pairs separated by the new line character. Optional
    #[clap(short, long, value_name = "PATH")]
    secrets: Option<PathBuf>,

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

        let (executable, connector_metadata) = from_cargo_package(self.package)
            .context("Failed to deploy from within cargo package directory")?;

        let mut builder = Deployment::builder();
        builder
            .executable(executable)
            .config(self.config)
            .secrets(self.secrets)
            .pkg(connector_metadata)
            .deployment_type(DeploymentType::Local { output_file: None });
        builder.deploy()?;
        Ok(())
    }
}
