use fluvio_hub_util::cmd::{ConnectorHubDownloadOpts, ConnectorHubListOpts};

use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;

/// List available Connectors in the hub
#[derive(Debug, Parser)]
pub enum ConnectorHubSubCmd {
    #[command(name = "list")]
    List(ConnectorHubListOpts),
    #[command(name = "download")]
    Download(ConnectorHubDownloadOpts),
}

impl ConnectorHubSubCmd {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        match self {
            ConnectorHubSubCmd::List(opts) => opts.process(out).await,
            ConnectorHubSubCmd::Download(opts) => opts.process(out).await,
        }
    }
}
