use std::fs::remove_dir_all;

use anyhow::Result;
use clap::Parser;
use dialoguer::Confirm;
use dialoguer::theme::ColorfulTheme;

use crate::GlobalOptions;

use crate::common::notify::Notify;
use crate::common::workdir::fvm_workdir_path;

#[derive(Clone, Debug, Parser)]
pub struct SelfUninstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Skip the confirmation prompt and uninstall FVM
    #[clap(long)]
    yes: bool,
}

impl SelfUninstallOpt {
    pub async fn process(&self) -> Result<()> {
        // Checks if FVM is already installed
        let workdir_path = fvm_workdir_path()?;

        if workdir_path.exists() {
            if self.yes
                || Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(&format!(
                        "Are you sure you want to uninstall FVM from {}?",
                        workdir_path.display()
                    ))
                    .interact()?
            {
                remove_dir_all(&workdir_path)?;
                self.notify_done(format!(
                    "Fluvio Version Manager was removed from {}",
                    workdir_path.display()
                ));
            }

            return Ok(());
        }

        self.notify_warn(format!(
            "Aborting uninstallation, no FVM installation found at {}",
            workdir_path.display()
        ));
        Ok(())
    }
}

impl Notify for SelfUninstallOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
