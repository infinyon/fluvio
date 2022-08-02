mod create;
mod list;
mod delete;

#[cfg(all(feature = "smartengine", not(target_os = "windows")))]
mod test;

pub use cmd::SmartModuleCmd;

mod cmd {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;

    use fluvio::Fluvio;
    use fluvio_extension_common::target::ClusterTarget;

    use crate::Result;
    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;

    use super::create::CreateSmartModuleOpt;
    use super::list::ListSmartModuleOpt;
    use super::delete::DeleteSmartModuleOpt;

    #[derive(Debug, Parser)]
    pub enum SmartModuleCmd {
        Create(CreateSmartModuleOpt),
        List(ListSmartModuleOpt),
        /// Delete one or more Smart Modules with the given name(s)
        Delete(DeleteSmartModuleOpt),

        #[cfg(all(feature = "smartengine", not(target_os = "windows")))]
        Test(super::test::TestSmartModuleOpt),
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
                #[cfg(all(feature = "smartengine", not(target_os = "windows")))]
                Self::Test(opt) => {
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
