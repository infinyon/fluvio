//! FVM Management Commands

pub mod install;
pub mod uninstall;

use anyhow::Result;
use clap::Parser;

use crate::GlobalOptions;
use crate::common::notify::Notify;

use self::install::SelfInstallOpt;
use self::uninstall::SelfUninstallOpt;

#[derive(Debug, Parser)]
pub enum ItselfCommand {
    /// Install `fvm` and setup the workspace
    Install(SelfInstallOpt),
    /// Uninstall `fvm` and removes the workspace
    Uninstall(SelfUninstallOpt),
}

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct SelfOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Subcommand to execute
    #[clap(subcommand)]
    command: ItselfCommand,
}

impl SelfOpt {
    pub async fn process(&self) -> Result<()> {
        match &self.command {
            ItselfCommand::Install(cmd) => cmd.process().await?,
            ItselfCommand::Uninstall(cmd) => cmd.process().await?,
        }

        Ok(())
    }
}

impl Notify for SelfOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
