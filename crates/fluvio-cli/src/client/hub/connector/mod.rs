use fluvio_hub_util::cmd::{ConnectorHubDownloadOpts, ConnectorHubListOpts};

use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;

const CONNECTOR_HUB_DOWNLOAD_REDIRECT_MESSAGE: &str = r#"Connectors running on `InfinyOn Cloud` are automatically downloaded during `fluvio cloud connector create ...`.
Connectors running locally require `cdk` to download and deploy:
1. Install cdk: `fluvio install cdk`
2. Download connector: `cdk hub download ...`
3. Deploy connector: `cdk deploy start ...`"#;

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
                out.println(CONNECTOR_HUB_DOWNLOAD_REDIRECT_MESSAGE);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{RwLock, Arc};

    use clap::Parser;
    use fluvio_extension_common::Terminal;
    use fluvio_future::task::run_block_on;

    use crate::client::hub::connector::CONNECTOR_HUB_DOWNLOAD_REDIRECT_MESSAGE;

    use super::ConnectorHubSubCmd;

    #[derive(Default, Debug)]
    struct MockTerminal(Arc<RwLock<String>>);
    impl Terminal for MockTerminal {
        fn print(&self, msg: &str) {
            self.0
                .write()
                .expect("could not print to MockTerminal")
                .push_str(msg);
        }

        fn println(&self, msg: &str) {
            self.0
                .write()
                .expect("could not println to MockTerminal")
                .push_str(&format!("{msg}\n"));
        }
    }

    #[test]
    fn test_calling_fluvio_hub_download_displays_a_help_message() {
        let terminal = Arc::new(MockTerminal::default());
        let cmd =
            ConnectorHubSubCmd::parse_from(["conn", "download", "infinyon/duckdb-sink@0.1.0"]);

        assert!(matches!(cmd, ConnectorHubSubCmd::Download(_)));

        run_block_on(cmd.process(terminal.clone())).expect("command failed to process");

        let x = terminal.0.read().unwrap();
        let x = x.as_str().trim_end();
        assert_eq!(x, CONNECTOR_HUB_DOWNLOAD_REDIRECT_MESSAGE);
    }
}
