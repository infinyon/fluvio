//!
//! # Delete Managed Connectors
//!
//! CLI tree to generate Delete Managed Connectors
//!
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::connector::{ManagedConnectorSpec};

use crate::error::ConnectorError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedConnectorOpt {
    /// The name of the connector to delete
    #[structopt(value_name = "name")]
    name: String,
}

impl DeleteManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ConnectorError> {
        let admin = fluvio.admin().await;
        admin.delete::<ManagedConnectorSpec, _>(&self.name).await?;
        Ok(())
    }
}
