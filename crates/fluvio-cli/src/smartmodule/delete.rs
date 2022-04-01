use clap::Parser;
use crate::Result;
use fluvio::Fluvio;
use fluvio::metadata::smartmodule::SmartModuleSpec;

/// Delete an existing SmartModule with the given name
#[derive(Debug, Parser)]
pub struct DeleteSmartModuleOpt {
    name: String,
}

impl DeleteSmartModuleOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        admin.delete::<SmartModuleSpec, _>(&self.name).await?;
        Ok(())
    }
}
