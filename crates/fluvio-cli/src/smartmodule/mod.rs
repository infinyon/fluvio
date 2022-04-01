use std::sync::Arc;
use clap::Parser;
use crate::Result;
use crate::common::output::Terminal;
use fluvio::Fluvio;

mod create;
mod list;
mod describe;
mod delete;

use self::create::CreateSmartModuleOpt;
use self::list::ListSmartModuleOpt;
use self::describe::DescribeSmartModuleOpt;
use self::delete::DeleteSmartModuleOpt;

#[derive(Debug, Parser)]
pub enum SmartModuleCmd {
    Create(CreateSmartModuleOpt),
    List(ListSmartModuleOpt),
    Describe(DescribeSmartModuleOpt),
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
            Self::Describe(opt) => {
                opt.process()?;
            }
            Self::Delete(opt) => {
                opt.process(fluvio).await?;
            }
        }
        Ok(())
    }
}
