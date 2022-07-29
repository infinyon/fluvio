mod create;
mod list;
mod delete;
mod test;

pub use cmd::SmartModuleCmd;

mod cmd {

    use std::sync::Arc;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use clap::Parser;

    use fluvio::Fluvio;

    use crate::Result;
    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;

    use super::create::CreateSmartModuleOpt;
    use super::list::ListSmartModuleOpt;
    use super::delete::DeleteSmartModuleOpt;
    use super::test::TestSmartModuleOpt;

    #[derive(Debug, Parser)]
    pub enum SmartModuleCmd {
        Create(CreateSmartModuleOpt),
        List(ListSmartModuleOpt),
        /// Delete one or more Smart Modules with the given name(s)
        Delete(DeleteSmartModuleOpt),
        Test(TestSmartModuleOpt)
    }

    #[async_trait]
    impl ClientCmd for SmartModuleCmd {
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
            match self {
                Self::Create(opt) => {
                    opt.process(fluvio).await?;
                }
                Self::List(opt) => {
                    opt.process(out, fluvio).await?;
                }
                Self::Delete(opt) => {
                    opt.process(fluvio).await?;
                }
                Self::Test(opt) => {
                    opt.process(fluvio).await?;
                }
            }
            Ok(())
        }
    }
}
