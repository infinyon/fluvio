mod local;

use std::path::PathBuf;

use anyhow::{Result, Context};
use derive_builder::Builder;

use fluvio_connector_package::metadata::ConnectorMetadata;

#[derive(Clone)]
pub enum DeploymentType {
    Local { output_file: Option<PathBuf> },
}

/// Describe deployment configuration
#[derive(Builder)]
pub struct Deployment {
    pub executable: PathBuf, // path to executable
    #[builder(default)]
    pub secrets: Option<PathBuf>, // path to secrets file
    pub config: PathBuf,     // Configuration to pass along,
    pub pkg: ConnectorMetadata, // Connector pkg definition
    pub deployment_type: DeploymentType, // deployment type
}

impl Deployment {
    pub fn builder() -> DeploymentBuilder {
        DeploymentBuilder::default()
    }
}

#[derive(Debug)]
pub enum DeploymentResult {
    Local {
        process_id: u32,
        name: String,
        log_file: Option<PathBuf>,
    },
}

impl DeploymentBuilder {
    fn config_file_error(err: std::io::Error, config: PathBuf) -> Result<DeploymentResult> {
        Err(err).with_context(|| {
            format!(
                "Could not open connector config at: {}. \
                    \n Please provide a connector config file with \"--config\" \
                    \n or use the default path: sample-config.yaml \
                    \n\n For instructions on creating connector config files, \
                    \n see: https://www.fluvio.io/connectors/connector-config/",
                config.display(),
            )
        })
    }

    pub fn deploy(self) -> Result<DeploymentResult> {
        let deployment = self.build()?;

        let config_file = match std::fs::File::open(&deployment.config) {
            Ok(file) => file,
            Err(err) => {
                return Self::config_file_error(err, deployment.config);
            }
        };

        let config = deployment.pkg.validate_config(config_file)?;
        match &deployment.deployment_type {
            DeploymentType::Local { output_file } => {
                let process_id = local::deploy_local(&deployment, output_file.as_ref())?;
                let name = config.meta().name.to_owned();
                let log_file = output_file.clone();
                Ok(DeploymentResult::Local {
                    process_id,
                    name,
                    log_file,
                })
            }
        }
    }
}
