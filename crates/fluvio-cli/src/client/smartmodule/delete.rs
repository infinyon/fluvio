use std::sync::Arc;
use std::fmt::Debug;

use async_trait::async_trait;

use tracing::debug;
use clap::Parser;

use fluvio::Fluvio;
use fluvio_extension_common::Terminal;
use fluvio::metadata::smartmodule::SmartModuleSpec;

use crate::Result;
use crate::client::cmd::ClientCmd;
use crate::error::CliError;

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

#[async_trait]
impl ClientCmd for DeleteSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<()> {
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
