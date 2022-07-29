use clap::Parser;
use crate::Result;
use crate::error::CliError;
use fluvio::Fluvio;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use tracing::debug;

/// Delete an existing SmartModule with the given name
#[derive(Debug, Parser)]
pub struct DeleteSmartModuleOpt {
    /// Continue deleting in case of an error
    #[clap(short, long, action, required = false)]
    continue_on_error: bool,
    /// One or more name(s) of the smart module(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteSmartModuleOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let mut err_happened = false;
        for name in self.names.iter() {
            debug!(name, "deleting smart module");
            if let Err(error) = admin.delete::<SmartModuleSpec, _>(name).await {
                let error = CliError::from(error);
                err_happened = true;
                if self.continue_on_error {
                    let user_error = match error.get_user_error() {
                        Ok(usr_err) => usr_err.to_string(),
                        Err(err) => format!("{}", err),
                    };
                    println!(
                        "smart module \"{}\" delete failed with: {}",
                        name, user_error
                    );
                } else {
                    return Err(error);
                }
            } else {
                println!("smart module \"{}\" deleted", name);
            }
        }
        if err_happened {
            Err(CliError::CollectedError(
                "Failed deleting smart module(s). Check previous errors.".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}
