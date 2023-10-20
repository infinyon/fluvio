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
            ConnectorHubSubCmd::Download(_) => {
                out.println("Connectors running on `InfinyOn Cloud` are automatically downloaded during `fluvio cloud connector create ...`.");
                out.println("Connectors running locally require `cdk` to download and deploy:");
                out.println("1. Install cdk: `fluvio install cdk`");
                out.println("2. Download connector: `cdk hub download ...`");
                out.println("3. Deploy connector: `cdk deploy start ...`");
                Ok(())
            }
        }
    }
}
