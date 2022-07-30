mod create;
mod list;
mod delete;

pub use cmd::SmartModuleCmd;

mod cmd {

    use std::sync::Arc;

    use clap::Parser;
    use fluvio::Fluvio;

    use crate::Result;
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
    }

    impl SmartModuleCmd {
        pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
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
            }
            Ok(())
        }
    }
}
