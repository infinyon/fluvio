use std::fs::remove_dir_all;

use clap::Parser;
use color_eyre::eyre::Result;
use color_eyre::owo_colors::OwoColorize;
use dialoguer::Confirm;
use dialoguer::theme::ColorfulTheme;

use crate::GlobalOptions;

use crate::common::notify::Notify;
use crate::common::workdir::fvm_workdir_path;

#[derive(Clone, Debug, Parser)]
pub struct UninstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl UninstallOpt {
    pub async fn process(&self) -> Result<()> {
        // Checks if FVM is already installed
        let workdir_path = fvm_workdir_path()?;

        if workdir_path.exists() {
            if Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt(&format!(
                    "Are you sure you want to uninstall FVM from {}?",
                    workdir_path.display().italic()
                ))
                .interact()?
            {
                remove_dir_all(workdir_path)?;
            }

            return Ok(());
        }

        self.notify_warn(format!(
            "Aborting uninstallation, no FVM installation found at {}",
            workdir_path.display().italic()
        ));

        Ok(())
    }
}

impl Notify for UninstallOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
