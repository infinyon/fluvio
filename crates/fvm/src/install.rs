//! Fluvio Version Manager Install Utilities
//!
//! When FVM is first installed, it needs to be initialized. This is achieved by
//! preparing the workspace which involves creating the `.fvm` directory and
//! providing the `fvm` binary in the `~/.fvm/bin` directory.
//!
//! This is likely to run once and automatically by the installer after
//! downloading the FVM binary.

use std::env::current_exe;
use std::fs::{copy, create_dir};
use std::path::PathBuf;

use crate::{Error, Result};
use crate::common::{FVM_BINARY_NAME, FVM_HOME_DIR, FVM_PACKAGES_SET_DIR};

/// Retrieves the path to the `~/.fvm` directory in the host system.
/// This function only builds the path, it doesn't check if the directory exists.
pub fn fvm_path() -> Result<PathBuf> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };
    let fvm_path = home_dir.join(FVM_HOME_DIR);

    Ok(fvm_path)
}

/// Checks if the FVM is installed. This is achieved by checking if the
/// binary is present in the `FVM` home directory.
///
/// The returned path is the path to the FVM binary found.
pub fn fvm_bin_path() -> Result<Option<PathBuf>> {
    let fvm_path = fvm_path()?;
    let fvm_binary_path = fvm_path.join("bin").join(FVM_BINARY_NAME);

    if fvm_binary_path.exists() {
        return Ok(Some(fvm_binary_path));
    }

    Ok(None)
}

/// Retrieves the path to the `~/.fvm/pkgset` directory in the host system.
pub fn fvm_pkgset_path() -> Result<PathBuf> {
    let fvm_path = fvm_path()?;
    let fvm_pkget_path = fvm_path.join(FVM_PACKAGES_SET_DIR);

    Ok(fvm_pkget_path)
}

/// Installs FVM in the host system.
///
/// Intallation process consists in creating the FVM home `bin` directory, and
/// copying the FVM binary to the FVM home `bin` directory.
///
/// This is executed on the first installation of FVM, similar to hom `rustup`
/// does when installed.
///
/// Returs the path to the directory where FVM was installed.
pub fn install_fvm() -> Result<PathBuf> {
    // Creates the directory `~/.fvm` if doesn't exists
    let fvm_dir = fvm_path()?;

    if !fvm_dir.exists() {
        create_dir(&fvm_dir).map_err(|err| Error::Setup(err.to_string()))?;
        tracing::debug!(?fvm_dir, "Created FVM home directory");
    }

    // Attempts to create the binary crate
    let fvm_binary_dir = fvm_dir.join("bin");
    create_dir(fvm_binary_dir).map_err(|err| Error::Setup(err.to_string()))?;

    // Copies "this" binary to the FVM binary directory
    let current_binary_path = current_exe().map_err(|err| Error::Setup(err.to_string()))?;
    let fvm_binary_path = fvm_dir.join("bin").join("fvm");

    copy(current_binary_path, fvm_binary_path).map_err(|err| Error::Setup(err.to_string()))?;
    tracing::debug!(?fvm_dir, "Copied the FVM binary to the FVM home directory");

    // Creates the package set directory
    let fvm_pkgset_dir = fvm_dir.join(FVM_PACKAGES_SET_DIR);
    create_dir(fvm_pkgset_dir).map_err(|err| Error::Setup(err.to_string()))?;

    Ok(fvm_dir)
}
