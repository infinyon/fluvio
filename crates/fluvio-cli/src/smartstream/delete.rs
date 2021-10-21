use structopt::StructOpt;
use crate::Result;
use fluvio::Fluvio;
use fluvio::metadata::smartstream::SmartStreamSpec;

#[derive(Debug, StructOpt)]
pub struct DeleteSmartStreamOpt {
    name: String,
}

impl DeleteSmartStreamOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        admin.delete::<SmartStreamSpec, _>(&self.name).await?;
        Ok(())
    }
}
