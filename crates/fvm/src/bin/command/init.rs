//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;

use fluvio_version_manager::init::{is_fvm_installed, install_fvm};
use fluvio_version_manager::notify::Notify;

/// The `init` command is responsible of preparing the workspace for FVM.
pub struct InitCommand {
    quiet: bool,
}

impl InitCommand {
    pub fn new(quiet: bool) -> Self {
        Self { quiet }
    }

    pub fn exec(&self) -> Result<()> {
        self.notify_info("Checking for existent FVM installation");

        if is_fvm_installed()? {
            self.notify_warning("Detected FVM installation, skipping installation");
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;

        self.notify_success("FVM installed successfully");
        Ok(())
    }
}

impl Notify for InitCommand {
    fn command(&self) -> &'static str {
        "init"
    }

    fn is_quiet(&self) -> bool {
        self.quiet
    }
}
