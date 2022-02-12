//!
//! # Get the logs for Managed Connectors
//!
//! CLI tree to logs for managed connectors
//!
use std::process::Command;
use structopt::StructOpt;
use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct LogsManagedConnectorOpt {
    /// The name of the connector to view the logs
    #[structopt(value_name = "name")]
    name: String,

    /// Follow the logs
    #[structopt(long, short)]
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
        let pod = pods.split(' ').find(|pod| pod.starts_with(&self.name));

        if let Some(pod) = pod {
            println!();
            let args = if self.follow {
                vec!["logs", "-f", pod]
            } else {
                vec!["logs", pod]
            };
            Command::new("kubectl").args(args).spawn()?.wait()?;
        }
        Ok(())
    }
}
