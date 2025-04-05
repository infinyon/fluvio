use anyhow::Result;
use clap::Parser;

use crate::fluvio::cli::FluvioServerCommand;

#[derive(Debug, Parser)]
pub(crate) enum RootCommand {
    Server(FluvioServerCommand),
}

impl RootCommand {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::Server(opt) => opt.process().await,
        }
    }
}
