//! The `current` command is responsible of printing the active Fluvio Version

use color_eyre::eyre::Result;
use colored::Colorize;
use clap::Parser;

use fluvio_hub_util::fvm::Channel;
use fluvio_version_manager::settings::Settings;
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

#[derive(Debug, Parser)]
pub struct ShowOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl ShowOpt {
    pub async fn process(&self) -> Result<()> {
        Ok(())
    }
}

impl Notify for ShowOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
