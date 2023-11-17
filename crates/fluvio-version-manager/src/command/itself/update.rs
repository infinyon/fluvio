use anyhow::Result;
use clap::Parser;

use crate::common::notify::Notify;

#[derive(Clone, Debug, Parser)]
pub struct SelfUpdateOpt;

impl SelfUpdateOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        notify.info("To update fvm, please run:");
        notify.info("curl -fsS https://hub.infinyon.cloud/install/install.sh | bash");
        Ok(())
    }
}
