//! Fluvio Version Manager Initialization Utilities
//!
//! When FVM is first installed, it needs to be initialized. This is achieved by
//! preparing the workspace by creating the `.fvm` directory and providing the
//! `fvm` binary in the `~/.fvm/bin` directory.
//!
//! This is likely to run once and automatically by the installer after
//! downloading the FVM binary.

use std::env::current_exe;
use std::fs::{copy, create_dir};

use crate::constants;
use crate::Error;

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
///
/// Intallation process consists in creating the FVM home `bin` directory, and
/// copying the FVM binary to the FVM home `bin` directory.
///
/// This is executed on the first installation of FVM, similar to hom `rustup`
/// does when installed.
pub fn install_fvm() -> Result<(), Error> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };

    // Creates the directory `~/.fvm` if doesn't exists
    let fvm_dir = home_dir.join(constants::FVM_HOME_DIR);

    if !fvm_dir.exists() {
        create_dir(&fvm_dir).map_err(|err| Error::InitFailed(err.to_string()))?;
        tracing::debug!(?fvm_dir, "Created FVM home directory");
    }

    // Attempts to create the binary crate
    let fvm_binary_dir = fvm_dir.join("bin");
    create_dir(&fvm_binary_dir).map_err(|err| Error::InitFailed(err.to_string()))?;

    // Copies "this" binary to the FVM binary directory
    let current_binary_path = current_exe().map_err(|err| Error::InitFailed(err.to_string()))?;
    let fvm_binary_path = fvm_dir.join("bin").join("fvm");

    copy(&current_binary_path, &fvm_binary_path)
        .map_err(|err| Error::InitFailed(err.to_string()))?;
    tracing::debug!(?fvm_dir, "Copied the FVM binary to the FVM home directory");

    // Creates the package set directory
    let fvm_pkgset_dir = fvm_dir.join(constants::FVM_PACKAGES_SET_DIR);
    create_dir(&fvm_pkgset_dir).map_err(|err| Error::InitFailed(err.to_string()))?;

    Ok(())
}
