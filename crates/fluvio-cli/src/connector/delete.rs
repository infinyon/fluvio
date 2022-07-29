//!
//! # Delete Managed Connectors
//!
//! CLI tree to generate Delete Managed Connectors
//!
use clap::Parser;
use fluvio::Fluvio;
use fluvio::metadata::connector::ManagedConnectorSpec;
use crate::CliError;
use tracing::debug;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DeleteManagedConnectorOpt {
    /// ignore delete errors if any
    #[clap(short, long, action, required = false)]
    ignore_error: bool,
    /// The name(s) of the connector(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        for name in self.names.iter() {
            debug!("deleting connector: {}", name);
            if let Err(error) = admin.delete::<ManagedConnectorSpec, _>(name).await {
                if self.ignore_error {
                    println!("connector \"{}\" delete failed with: {}", name, error);
                } else {
                    return Err(error.into());
                }
            } else {
                println!("connector \"{}\" deleted", name);
            }
        }

        Ok(())
    }
}
