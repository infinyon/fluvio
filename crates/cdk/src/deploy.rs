use std::{fmt::Debug, sync::Arc};

use anyhow::{Result};
use clap::Parser;

use cargo_builder::package::{PackageInfo};
use fluvio_connector_deployer::{Deployment, DeploymentType};

use crate::build::PackageCmd;

/// Builds the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct Deploy {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

impl Deploy {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let _p = PackageInfo::from_options(&opt).map_err(|e| anyhow::anyhow!(e))?;

        // TODO
        // find binary
        // spawn connector process and pass config file
        let mut builder = Deployment::builder();
        builder
            .executable("target/debug/fluvio-connector-echo".into())
            // .config(Default::default())
            // .pkg(Default::default())
            .deployment_type(DeploymentType::Local);
        builder.deploy()?;

        Ok(())
    }
}
