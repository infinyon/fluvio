use std::sync::Arc;
use clap::Parser;

mod list;
mod display;
mod register;
mod unregister;

use fluvio::Fluvio;
// pub use display::*;

use crate::cli::ClusterCliError;
use super::common::COMMAND_TEMPLATE;
use super::common::output::Terminal;
use list::ListSpusOpt;
use register::RegisterCustomSpuOpt;
use unregister::UnregisterCustomSpuOpt;

#[derive(Debug, Parser)]
pub enum SpuCmd {
    /// Register a new custom SPU with the cluster
    #[clap(
        name = "register",
        help_template = COMMAND_TEMPLATE,
    )]
    Register(RegisterCustomSpuOpt),

    /// Unregister a custom SPU from the cluster
    #[clap(
        name = "unregister",
        help_template = COMMAND_TEMPLATE,
    )]
    Unregister(UnregisterCustomSpuOpt),

    /// List all SPUs known by this cluster (managed AND custom)
    #[clap(
        name = "list",
        help_template = COMMAND_TEMPLATE,
    )]
    List(ListSpusOpt),
}

impl SpuCmd {
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<(), ClusterCliError> {
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
