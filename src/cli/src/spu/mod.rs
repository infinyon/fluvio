use std::sync::Arc;
use structopt::StructOpt;

mod list;
mod display;
mod register;
mod unregister;

use fluvio::Fluvio;
pub use display::*;
use crate::Result;
use crate::common::COMMAND_TEMPLATE;
use crate::common::output::Terminal;
use crate::spu::list::ListSpusOpt;
use crate::spu::register::RegisterCustomSpuOpt;
use crate::spu::unregister::UnregisterCustomSpuOpt;

#[derive(Debug, StructOpt)]
pub enum SpuCmd {
    /// Registers a new custom SPU with the cluster
    #[structopt(
        name = "register",
        template = COMMAND_TEMPLATE,
    )]
    Register(RegisterCustomSpuOpt),

    /// Unregisters a custom SPU from the cluster
    #[structopt(
        name = "unregister",
        template = COMMAND_TEMPLATE,
    )]
    Unregister(UnregisterCustomSpuOpt),

    /// List all SPUs known by this cluster (managed AND custom)
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListSpusOpt),
}

impl SpuCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::Register(register) => {
                register.process(fluvio).await?;
            }
            Self::Unregister(unregister) => {
                unregister.process(fluvio).await?;
            }
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}
