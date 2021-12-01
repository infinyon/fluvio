//!
//! # Delete Managed Connectors
//!
//! CLI tree to generate Delete Managed Connectors
//!
use structopt::StructOpt;
use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct LogsManagedConnectorOpt {
    /// The name of the connector to delete
    #[structopt(value_name = "name")]
    name: String,
}
use std::process::Command;

impl LogsManagedConnectorOpt {
    pub async fn process(self) -> Result<(), CliError> {
        let pods = Command::new("kubectl")
            .args(["get", "pods", "-o=jsonpath='{.items[*].metadata.name}"])
            .output()?
            .stdout;
        let pods = String::from_utf8_lossy(&pods);
        let pod = pods.split(' ').find(|pod| pod.starts_with(&self.name));

        if let Some(pod) = pod {
            println!();
            Command::new("kubectl")
                .args(["logs", "-f", pod])
                .spawn()?
                .wait()?;
        }
        Ok(())
    }
}
