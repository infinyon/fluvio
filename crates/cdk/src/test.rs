use std::{fmt::Debug, path::PathBuf};

use anyhow::{Result, Context};
use clap::Parser;

use cargo_builder::package::PackageInfo;
use fluvio_connector_deployer::{Deployment, DeploymentType};

use crate::cmd::PackageCmd;
use crate::deploy::from_cargo_package;
use crate::utils::build::{build_connector, BuildOpts};

/// Build and run the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct TestCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Path to configuration file in YAML format
    #[arg(short, long, value_name = "PATH", default_value = "sample-config.yaml")]
    config: PathBuf,

    /// Path to file with secrets. Secrets are 'key=value' pairs separated by the new line character. Optional
    #[arg(short, long, value_name = "PATH")]
    secrets: Option<PathBuf>,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,
}

impl TestCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)?;
        let build_options = BuildOpts {
            release: opt.release,
            extra_arguments: self.extra_arguments,
        };

        build_connector(&package_info, build_options)?;

        let (executable, connector_metadata) = from_cargo_package(&package_info)
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
