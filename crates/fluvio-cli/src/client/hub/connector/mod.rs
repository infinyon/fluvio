mod list;
use list::ConnectorHubListOpts;
mod download;
use download::ConnectorHubDownloadOpts;

use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;

use super::{get_pkg_list, get_hub_access};

/// List available Connectors in the hub
#[derive(Debug, Parser)]
pub enum ConnectorHubSubCmd {
    /// List all available SmartConnectors
    #[command(name = "list")]
    List(ConnectorHubListOpts),

    /// Download SmartConnector to the local folder
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
