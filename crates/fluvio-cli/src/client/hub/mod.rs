pub use cmd::HubCmd;

mod connector;
mod smartmodule;
pub use smartmodule::{download_local, download_cluster};

mod cmd {
    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;
    use anyhow::Result;

    use fluvio::Fluvio;
    use fluvio_extension_common::target::ClusterTarget;

    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;

    use super::connector::ConnectorHubSubCmd;
    use super::smartmodule::SmartModuleHubSubCmd;

    #[derive(Debug, Parser)]
    pub enum HubCmd {
        #[clap(name = "smartmodule", visible_alias = "sm")]
        #[command(subcommand)]
        SmartModule(SmartModuleHubSubCmd),

        #[clap(visible_alias = "conn")]
        #[command(subcommand)]
        Connector(ConnectorHubSubCmd),
    }

    #[async_trait]
    impl ClientCmd for HubCmd {
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            _target: ClusterTarget,
        ) -> Result<()> {
            match self {
                Self::Connector(subcmd) => {
                    subcmd.process(out).await?;
                }

                Self::SmartModule(subcmd) => {
                    subcmd.process(out).await?;
                }
            }
            Ok(())
        }

        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            _out: Arc<O>,
            _fluvio: &Fluvio,
        ) -> Result<()> {
            Ok(())
        }
    }
}
