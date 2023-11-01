use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use fluvio_future::task;
use fluvio_hub_util::cmd::{ConnectorHubListOpts, ConnectorHubDownloadOpts};
use fluvio_extension_common::PrintTerminal;

/// Work with Connectors hub
#[derive(Debug, Parser)]
pub enum HubCmd {
    #[command(name = "list")]
    List(ConnectorHubListOpts),
    #[command(name = "download")]
    Download(ConnectorHubDownloadOpts),
}

impl HubCmd {
    pub fn process(self) -> Result<()> {
        let terminal = Arc::new(PrintTerminal::new());
        match self {
            HubCmd::List(opt) => task::run_block_on(opt.process(terminal)),
            HubCmd::Download(opt) => task::run_block_on(opt.process(terminal)),
        }
    }
}
