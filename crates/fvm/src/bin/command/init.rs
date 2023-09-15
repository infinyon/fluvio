//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;
use color_eyre::owo_colors::OwoColorize;
use clap::Parser;

use fluvio_version_manager::setup::{is_fvm_installed, install_fvm};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `init` command is responsible of preparing the workspace for FVM.
#[derive(Debug, Parser)]
pub struct InitOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl InitOpt {
    pub fn process(&self) -> Result<()> {
        self.notify_info("Checking for existent FVM installation");

        if let Some(installed_fvm_path) = is_fvm_installed()? {
            self.notify_warning(&format!(
                "Detected FVM installation at: {:?}, skipping installation",
                installed_fvm_path.italic()
            ));
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;

        self.notify_success("FVM installed successfully");
        Ok(())
    }
}

impl Notify for InitOpt {
    fn command(&self) -> &'static str {
        "init"
    }

    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
