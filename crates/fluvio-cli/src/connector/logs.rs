//!
//! # Get the logs for Managed Connectors
//!
//! CLI tree to logs for managed connectors
//!
use std::process::Command;
use clap::Parser;
use crate::CliError;

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
        let pods = Command::new("kubectl")
            .args([
                "get",
                "pods",
                "--selector",
                "app=fluvio-connector",
                "-o=jsonpath={.items[*].metadata.name}",
            ])
            .output()?
            .stdout;

        let pods = String::from_utf8_lossy(&pods);
        let pod = if !pods.is_empty() {
            pods.split(' ')
                .find(|pod_name| self.is_matching_connector_name(pod_name))
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

    fn is_matching_connector_name(&self, pod_name: &str) -> bool {
        const GENERATED_STRING_SIZE: usize = 17;

        let connector_name =
            &pod_name[..pod_name.len() - pod_name.len().min(GENERATED_STRING_SIZE)];
        connector_name == self.name
    }
}
