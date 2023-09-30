//! FVM Management Commands

pub mod install;
pub mod uninstall;

use color_eyre::eyre::Result;
use clap::Parser;

use crate::GlobalOptions;
use crate::common::notify::Notify;

use self::install::InstallOpt;
use self::uninstall::UninstallOpt;

#[derive(Debug, Parser)]
pub enum ItselfCommand {
    /// Install `fvm` and setup the workspace
    Install(InstallOpt),
    /// Uninstall `fvm` and removes the workspace
    Uninstall(UninstallOpt),
}

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct ItselfOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Subcommand to execute
    #[clap(subcommand)]
    command: ItselfCommand,
}

impl ItselfOpt {
    pub async fn process(&self) -> Result<()> {
        match &self.command {
            ItselfCommand::Install(cmd) => cmd.process().await?,
            ItselfCommand::Uninstall(cmd) => cmd.process().await?,
        }

        Ok(())
    }
}

impl Notify for ItselfOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
