//!
//! # Delete Managed Connectors
//!
//! CLI tree to generate Delete Managed Connectors
//!
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::connector::ManagedConnectorSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedConnectorOpt {
    /// The name of the connector to delete
    #[structopt(value_name = "name")]
    name: String,
}

use fluvio::FluvioError;
use fluvio_sc_schema::ApiError;
use fluvio_sc_schema::errors::ErrorCode;

impl DeleteManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;

        admin
            .delete::<ManagedConnectorSpec, _>(&self.name)
            .await
            .map_err(|e| match e {
                FluvioError::AdminApi(ApiError::Code(ErrorCode::ManagedConnectorNotFound, _)) => {
                    CliError::InvalidConnector(format!("{} not found", &self.name))
                }
                _ => CliError::Other(format!("{:?}", e)),
            })?;

        Ok(())
    }
}
