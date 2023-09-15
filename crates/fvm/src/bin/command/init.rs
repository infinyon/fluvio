//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;
use color_eyre::owo_colors::OwoColorize;
use clap::Parser;

use fluvio_version_manager::init::{is_fvm_installed, install_fvm};
use fluvio_version_manager::notify::Notify;

/// The `init` command is responsible of preparing the workspace for FVM.
#[derive(Debug, Parser)]
pub struct InitOpt;

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
}
