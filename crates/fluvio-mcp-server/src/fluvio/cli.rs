use anyhow::Result;
use clap::Parser;

use crate::util::FluvioConnect;

use super::server::FluvioMcpServer;

#[derive(Debug, Parser)]
pub(crate) struct FluvioServerCommand {
    #[arg(short, long)]
    profile: Option<String>,

    /// if enabled, turn on sse,
    #[arg(short, long, default_value_t = false)]
    sse: bool,
}

impl FluvioServerCommand {
    pub(crate) async fn process(self) -> Result<()> {
        let fluvio = FluvioConnect::builder()
            .maybe_profile(self.profile)
            .build()
            .connect()
            .await?;

        FluvioMcpServer::start_stdio(fluvio).await
    }
}
