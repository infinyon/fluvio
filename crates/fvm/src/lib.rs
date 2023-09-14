//! Fluvio Version Manager (FVM) Library
//!
//! Reusable components for the FVM CLI, constants and domain logic is
//! provided in this library crate.

pub mod constants;

use std::env::current_exe;
use std::fs::{copy, create_dir};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to find the Home Directory")]
    HomeDirNotFound,
    #[error("Failed to initialize FVM. {0}")]
    InstallFailed(String),
}

/// Checks if the FVM is installed. This is achieved by checking if the
/// binary is present in the FVM home directory.
pub fn is_fvm_installed() -> Result<bool, Error> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };

    let fvm_binary_path = home_dir
        .join(constants::FVM_HOME_DIR)
        .join("bin")
        .join(constants::FVM_BINARY_NAME);

    if fvm_binary_path.exists() {
        return Ok(true);
    }

    Ok(false)
}

/// Installs FVM in the host system.
pub fn install_fvm() -> Result<(), Error> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };

    // Creates the directory `~/.fvm` if doesn't exists
    let fvm_dir = home_dir.join(constants::FVM_HOME_DIR);

    if !fvm_dir.exists() {
        create_dir(&fvm_dir).map_err(|err| Error::InstallFailed(err.to_string()))?;
    }

    // Attempts to create the binary crate
    let fvm_binary_dir = fvm_dir.join("bin");
    create_dir(&fvm_binary_dir).map_err(|err| Error::InstallFailed(err.to_string()))?;

    // Copies "this" binary to the FVM binary directory
    let current_binary_path = current_exe()
        .map_err(|err| Error::InstallFailed(err.to_string()))?
        .join(constants::FVM_BINARY_NAME);

    copy(&current_binary_path, &fvm_binary_dir)
        .map_err(|err| Error::InstallFailed(err.to_string()))?;

    Ok(())
}
