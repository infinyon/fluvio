use std::sync::Arc;
use structopt::StructOpt;
use crate::Result;
use crate::common::output::Terminal;
use fluvio::Fluvio;

mod create;
mod list;
mod describe;
mod delete;

use self::create::*;
use self::list::*;
use self::describe::*;
use self::delete::*;

#[derive(Debug, StructOpt)]
pub enum SmartStreamCmd {
    Create(CreateSmartStreamOpt),
    List(ListSmartStreamOpt),
    Describe(DescribeSmartStreamOpt),
    Delete(DeleteSmartStreamOpt),
}

impl SmartStreamCmd {
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
