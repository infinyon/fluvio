use std::{
    fmt::Debug,
    path::{PathBuf, Path},
};

use anyhow::Result;
use clap::{Parser, Subcommand};

use cargo_builder::package::PackageInfo;
use fluvio_connector_deployer::{Deployment, DeploymentType};
use fluvio_connector_package::{config::ConnectorConfig, metadata::ConnectorMetadata};

use crate::cmd::PackageCmd;

const CONNECTOR_METADATA_FILE_NAME: &str = "Connector.toml";

/// Deploys the Connector from the current working directory
#[derive(Debug, Parser)]
pub struct DeployCmd {
    #[clap(flatten)]
    package: PackageCmd,

    #[command(subcommand)]
    deployment_type: DeploymentTypeCmd,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum DeploymentTypeCmd {
    Local {
        #[clap(short, long, value_name = "PATH")]
        config: PathBuf,
    },
}

impl DeployCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt)?;
        let connector_metadata = ConnectorMetadata::from_toml_file(
            p.package_relative_path(CONNECTOR_METADATA_FILE_NAME),
        )?;

        let mut builder = Deployment::builder();
        builder
            .executable(p.target_bin_path()?)
            .config(ConnectorConfig::from_file(self.config())?)
            .pkg(connector_metadata)
            .deployment_type(self.deployment_type.into());
        builder.deploy()?;

        Ok(())
    }

    fn config(&self) -> &Path {
        match &self.deployment_type {
            DeploymentTypeCmd::Local { config } => config.as_path(),
        }
    }
}

impl From<DeploymentTypeCmd> for DeploymentType {
    fn from(cmd: DeploymentTypeCmd) -> Self {
        match cmd {
            DeploymentTypeCmd::Local { .. } => DeploymentType::Local,
        }
    }
}
