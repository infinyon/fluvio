pub use cmd::HubCmd;

mod connector;
mod download;
mod list;

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
    use super::download::DownloadHubOpt;
    use super::list::ListHubOpt;

    #[derive(Debug, Parser)]
    pub enum HubCmd {
        Download(DownloadHubOpt),
        List(ListHubOpt),

        #[clap(subcommand)]
        Connector(ConnectorHubSubCmd),
    }

    #[async_trait]
    impl ClientCmd for HubCmd {
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            target: ClusterTarget,
        ) -> Result<()> {
            match self {
                Self::Download(opt) => {
                    opt.process(out, target).await?;
                }
                Self::List(opt) => {
                    opt.process(out).await?;
                }
                Self::Connector(subcmd) => {
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

use anyhow::Result;

use fluvio_hub_util as hubutil;
use hubutil::HubAccess;
use hubutil::PackageListMeta;

use crate::{CliError};

pub(crate) fn get_hub_access(remote: &Option<String>) -> Result<HubAccess> {
    let access = HubAccess::default_load(remote).map_err(|_| {
        CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
    })?;
    Ok(access)
}

pub(crate) async fn get_pkg_list(
    endpoint: &str,
    remote: &Option<String>,
) -> Result<PackageListMeta> {
    use hubutil::http;

    let access = get_hub_access(remote)?;

    let action_token = access.get_list_token().await.map_err(|_| {
        CliError::HubError("rejected access credentials, try 'fluvio cloud login'".into())
    })?;
    let url = format!("{}/{endpoint}", &access.remote);
    let mut res = http::get(&url)
        .header("Authorization", &action_token)
        .await
        .map_err(|e| CliError::HubError(format!("list api access error {e}")))?;
    let pl: PackageListMeta = res
        .body_json()
        .await
        .map_err(|e| CliError::HubError(format!("list api data parse error {e}")))?;
    Ok(pl)
}
