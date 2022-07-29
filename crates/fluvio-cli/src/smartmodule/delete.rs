use clap::Parser;
use crate::Result;
use fluvio::Fluvio;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use tracing::debug;

/// Delete an existing SmartModule with the given name
#[derive(Debug, Parser)]
pub struct DeleteSmartModuleOpt {
    /// Ignore delete errors if any
    #[clap(short, long, action, required = false)]
    ignore_error: bool,
    /// The name(s) of the smart module(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteSmartModuleOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        for name in self.names.iter() {
            debug!("deleting smart module: {}", name);
            if let Err(error) = admin.delete::<SmartModuleSpec, _>(name).await {
                if self.ignore_error {
                    println!("smart module \"{}\" delete failed with: {}", name, error);
                } else {
                    return Err(error.into());
                }
            } else {
                println!("smart module \"{}\" deleted", name);
            }
        }
        Ok(())
    }
}
