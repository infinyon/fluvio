mod local;

use std::path::PathBuf;

use anyhow::Result;
use derive_builder::Builder;

use fluvio_connector_package::config::ConnectorConfig;
use fluvio_connector_package::metadata::ConnectorMetadata;

#[derive(Clone)]
pub struct Secret(String);

#[derive(Clone)]
pub enum DeploymentType {
    Local,
    K8,
}

/// Describe deployment configuration
#[derive(Builder)]
pub struct Deployment {
    pub executable: PathBuf, // path to executable
    #[builder(default)]
    pub secrets: Vec<Secret>, // List of Secrets
    pub config: ConnectorConfig, // Configuration to pass along,
    pub pkg: ConnectorMetadata, // Connector pkg definition
    pub deployment_type: DeploymentType, // deployment type
}

impl Deployment {
    pub fn builder() -> DeploymentBuilder {
        DeploymentBuilder::default()
    }
}

impl DeploymentBuilder {
    pub fn deploy(self) -> Result<()> {
        let deployment = self.build()?;
        deployment.pkg.validate_config(&deployment.config)?;
        match deployment.deployment_type {
            DeploymentType::Local => local::deploy_local(&deployment)?,
            DeploymentType::K8 => {
                unimplemented!()
            }
        }

        Ok(())
    }
}
