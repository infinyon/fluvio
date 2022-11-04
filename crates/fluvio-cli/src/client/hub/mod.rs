pub use cmd::HubCmd;

mod download;
mod list;

mod cmd {
    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;

    use fluvio::Fluvio;
    use fluvio_extension_common::target::ClusterTarget;

    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;
    use crate::Result;

    use super::download::DownloadHubOpt;
    use super::list::ListHubOpt;

    #[derive(Debug, Parser)]
    pub enum HubCmd {
        Download(DownloadHubOpt),
        List(ListHubOpt),
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
