mod create;
mod list;
mod delete;
mod watch;

pub use cmd::SmartModuleCmd;

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

    use super::create::CreateSmartModuleOpt;
    use super::list::ListSmartModuleOpt;
    use super::delete::DeleteSmartModuleOpt;
    use super::watch::WatchSmartModuleOpt;

    #[derive(Debug, Parser)]
    pub enum SmartModuleCmd {
        Create(CreateSmartModuleOpt),
        List(ListSmartModuleOpt),
        Watch(WatchSmartModuleOpt),
        /// Delete one or more SmartModules with the given name(s)
        Delete(DeleteSmartModuleOpt),
    }

    #[async_trait]
    impl ClientCmd for SmartModuleCmd {
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            target: ClusterTarget,
        ) -> Result<()> {
            match self {
                Self::Create(opt) => {
                    opt.process(out, target).await?;
                }
                Self::List(opt) => {
                    opt.process(out, target).await?;
                }
                Self::Delete(opt) => {
                    opt.process(out, target).await?;
                }
                Self::Watch(opt) => {
                    opt.process(out, target).await?;
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
