//! Fluvio Version Manager (FVM) Management Command
//!
//! The `self` command installs and updates FVM in your system

mod install;

use color_eyre::eyre::Result;
use clap::{Parser, Subcommand};

use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

use self::install::InstallOpt;

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Installs `fvm` in your system. This is usually run on initial setup
    #[command(name = "install")]
    Install(InstallOpt),
}

#[derive(Debug, Parser)]
pub struct SelfOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Version to install
    #[clap(subcommand)]
    command: Command,
}

impl SelfOpt {
    pub async fn process(&self) -> Result<()> {
        match &self.command {
            Command::Install(cmd) => cmd.process().await,
        }
    }
}

impl Notify for SelfOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
