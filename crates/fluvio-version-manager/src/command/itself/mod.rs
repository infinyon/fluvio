//! FVM Management Commands

pub mod install;
pub mod uninstall;
pub mod update;

use anyhow::Result;
use clap::Parser;

use crate::common::notify::Notify;

use self::install::SelfInstallOpt;
use self::uninstall::SelfUninstallOpt;
use self::update::SelfUpdateOpt;

#[derive(Debug, Parser)]
pub enum ItselfCommand {
    /// Install `fvm` and setup the workspace
    #[clap(hide = true)]
    Install(SelfInstallOpt),
    /// Uninstall `fvm` and removes the workspace
    Uninstall(SelfUninstallOpt),
    /// Prints `fvm` update instructions
    Update(SelfUpdateOpt),
}

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct SelfOpt {
    /// Subcommand to execute
    #[clap(subcommand)]
    command: ItselfCommand,
}

impl SelfOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        match &self.command {
            ItselfCommand::Install(cmd) => cmd.process(notify).await?,
            ItselfCommand::Uninstall(cmd) => cmd.process(notify).await?,
            ItselfCommand::Update(cmd) => cmd.process(notify).await?,
        }

        Ok(())
    }
}
