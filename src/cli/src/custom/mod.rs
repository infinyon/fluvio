use std::sync::Arc;
use structopt::StructOpt;

mod register;
mod list;
mod unregister;

use register::RegisterCustomSpuOpt;
use unregister::UnregisterCustomSpuOpt;
use list::ListCustomSpusOpt;

use fluvio::Fluvio;
use crate::{Result, Terminal};

#[derive(Debug, StructOpt)]
pub enum CustomSpuOpt {
    /// Registers a new custom SPU with the cluster
    #[structopt(
        name = "register",
        template = crate::COMMAND_TEMPLATE,
    )]
    Create(RegisterCustomSpuOpt),

    /// Unregisters a custom SPU from the cluster
    #[structopt(
        name = "unregister",
        template = crate::COMMAND_TEMPLATE,
    )]
    Delete(UnregisterCustomSpuOpt),

    /// List all custom SPUs known by this cluster
    #[structopt(
        name = "list",
        template = crate::COMMAND_TEMPLATE,
    )]
    List(ListCustomSpusOpt),
}

impl CustomSpuOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            CustomSpuOpt::Create(create) => {
                create.process(fluvio).await?;
            }
            CustomSpuOpt::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            CustomSpuOpt::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}
