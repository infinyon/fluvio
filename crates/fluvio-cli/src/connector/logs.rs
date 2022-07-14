//!
//! # Get the logs for Managed Connectors
//!
//! CLI tree to logs for managed connectors
//!
use std::{process::Command};
use clap::Parser;
use crate::CliError;
use fluvio::{config::ConfigFile as FluvioConfigFile};

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct LogsManagedConnectorOpt {
    /// The name of the connector to view the logs
    #[clap(value_name = "name")]
    name: String,

    /// Follow the logs
    #[clap(long, short)]
    pub follow: bool,
}

impl LogsManagedConnectorOpt {
    pub async fn process(self) -> Result<(), CliError> {
        let config_file = FluvioConfigFile::load_default_or_new()?;
        let fluvio_config = config_file.config();
        let profile = fluvio_config.current_profile()?;
        if profile.cluster.contains("cloud") {
            return Err(CliError::InvalidArg(
                "fluvio connector logs does not work with fluvio-cloud. Use fluvio cloud connector logs instead".to_string(),
            ));
        }

        if Command::new("kubectl").output().is_err() {
            return Err(CliError::InvalidArg(
                "fluvio connector logs requires kubctl to be installed".to_string(),
            ));
        }

        let pods = Command::new("kubectl")
            .args([
                "get",
                "pods",
                "--selector",
                &format!("app=fluvio-connector,connectorName={}", self.name),
                "-o=jsonpath={.items[*].metadata.name}",
            ])
            .output()?
            .stdout;

        let pods = String::from_utf8_lossy(&pods);
        let pod = if !pods.is_empty() {
            pods.split(' ').next()
        } else {
            None
        };

        if let Some(pod) = pod {
            println!();
            let args = if self.follow {
                vec!["logs", "-f", pod]
            } else {
                vec!["logs", pod]
            };
            Command::new("kubectl").args(args).spawn()?.wait()?;
            Ok(())
        } else {
            Err(CliError::InvalidArg(format!(
                "Connector {} does not exist",
                self.name
            )))
        }
    }
}
