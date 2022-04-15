use std::sync::Arc;
use clap::Parser;
use crate::Result;
use crate::common::output::Terminal;
use fluvio::Fluvio;

mod create;
mod list;
mod delete;

use self::create::*;
use self::list::*;
use self::delete::*;

#[derive(Debug, Parser)]
pub enum DerivedStreamCmd {
    Create(CreateDerivedStreamOpt),
    List(ListDerivedStreamOpt),
    Delete(DeleteDerivedStreamOpt),
}

impl DerivedStreamCmd {
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
