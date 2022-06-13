use std::sync::Arc;
use clap::Parser;
use crate::Result;
use crate::common::output::Terminal;
use fluvio::Fluvio;

mod create;
mod list;
mod delete;
mod generate;

use self::create::CreateSmartModuleOpt;
use self::list::ListSmartModuleOpt;
use self::delete::DeleteSmartModuleOpt;
use self::generate::GenerateSmartModuleOpt;

#[derive(Debug, Parser)]
pub enum SmartModuleCmd {
    Create(CreateSmartModuleOpt),
    List(ListSmartModuleOpt),
    Delete(DeleteSmartModuleOpt),
    Generate(GenerateSmartModuleOpt),
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
            Self::Generate(opt) => {
                opt.process(fluvio).await?;
            }
        }
        Ok(())
    }
}
