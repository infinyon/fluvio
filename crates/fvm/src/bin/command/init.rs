//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;

use fluvio_version_manager::{is_fvm_installed, install_fvm};

pub fn exec() -> Result<()> {
    if is_fvm_installed()? {
        println!("FVM is already installed");
        return Ok(());
    }

    install_fvm()?;
    Ok(())
}
