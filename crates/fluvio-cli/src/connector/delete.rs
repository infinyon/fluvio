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
    /// Continue deleting in case of an error
    #[clap(short, long, action, required = false)]
    continue_on_error: bool,
    /// One or more name(s) of the connector(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        let mut err_happened = false;
        for name in self.names.iter() {
            debug!(name, "deleting connector" );
            if let Err(error) = admin.delete::<ManagedConnectorSpec, _>(name).await {
                let error = CliError::from(error);
                err_happened = true;
                if self.continue_on_error {
                    let user_error = match error.get_user_error() {
                        Ok(usr_err) => usr_err.to_string(),
                        Err(err) => format!("{}", err),
                    };
                    println!("connector \"{}\" delete failed with: {}", name, user_error);
                } else {
                    return Err(error);
                }
            } else {
                println!("connector \"{}\" deleted", name);
            }
        }
        if err_happened {
            Err(CliError::CollectedError(
                "Failed deleting connector(s). Check previous errors.".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}
