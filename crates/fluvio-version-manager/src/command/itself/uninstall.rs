use std::fs::remove_dir_all;

use anyhow::Result;
use clap::Parser;
use dialoguer::Confirm;
use dialoguer::theme::ColorfulTheme;

use crate::common::notify::Notify;
use crate::common::workdir::fvm_workdir_path;

#[derive(Clone, Debug, Parser)]
pub struct SelfUninstallOpt {
    /// Skip the confirmation prompt and uninstall FVM
    #[clap(long)]
    yes: bool,
}

impl SelfUninstallOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        // Checks if FVM is already installed
        let workdir_path = fvm_workdir_path()?;

        if workdir_path.exists() {
            if self.yes
                || Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!(
                        "Are you sure you want to uninstall FVM from {}?",
                        workdir_path.display()
                    ))
                    .interact()?
            {
                remove_dir_all(&workdir_path)?;
                notify.done(format!(
                    "Fluvio Version Manager was removed from {}",
                    workdir_path.display()
                ));
            }

            return Ok(());
        }

        notify.warn(format!(
            "Aborting uninstallation, no FVM installation found at {}",
            workdir_path.display()
        ));
        Ok(())
    }
}
