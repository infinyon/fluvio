use std::sync::Arc;
use structopt::StructOpt;

mod list;
mod display;

use fluvio::Fluvio;
pub use display::*;
use crate::{Result, Terminal};
use crate::spu::list::ListSpusOpt;

#[derive(Debug, StructOpt)]
pub enum SpuCmd {
    /// List all SPUs known by this cluster (managed AND custom)
    #[structopt(
        name = "list",
        template = crate::COMMAND_TEMPLATE,
    )]
    List(ListSpusOpt),
}

impl SpuCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}
