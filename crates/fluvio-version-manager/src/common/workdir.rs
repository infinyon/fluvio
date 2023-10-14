//! The `Workdir` is the directory used by Fluvio Version Manager (FVM) to
//! store its files and binaries.

use std::path::PathBuf;
use std::env::var;

use anyhow::Result;

use super::home_dir;

/// Home Directory for Fluvio
pub const FLUVIO_HOME_DIR: &str = ".fluvio";

/// Home Directory for the Fluvio Version Manager (FVM) CLI
pub const FVM_HOME_DIR: &str = ".fvm";

/// FVM Binary Name
pub const FVM_BINARY_NAME: &str = "fvm";

/// FVM Versions Directory Name
///
/// Here is where all the versions are stored
pub const FVM_VERSIONS_DIR: &str = "versions";

/// FVM Workdir Name Environment Variable
pub const FVM_WORKDIR_NAME_ENV_VAR: &str = "FVM_WORKDIR_NAME";

/// Retrieves the path to the `~/.fvm` directory in the host system
pub fn fvm_workdir_path() -> Result<PathBuf> {
    let fvm_path = home_dir()?;

    if let Ok(workdir_name) = var(FVM_WORKDIR_NAME_ENV_VAR) {
        tracing::warn!("Using custom FVM workdir name: {}", workdir_name);

        Ok(fvm_path.join(workdir_name))
    } else {
        Ok(fvm_path.join(FVM_HOME_DIR))
    }
}

/// Retrieves the path to the `~/.fvm/bin/fvm` directory in the host system
pub fn fvm_bin_path() -> Result<PathBuf> {
    Ok(fvm_workdir_path()?.join("bin").join(FVM_BINARY_NAME))
}

/// Retrieves the path to the `~/.fvm/versions` directory in the host system
pub fn fvm_versions_path() -> Result<PathBuf> {
    Ok(fvm_workdir_path()?.join(FVM_VERSIONS_DIR))
}

/// Retrieves the path to the `~/.fluvio` directory in the host system.
pub fn fluvio_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(FLUVIO_HOME_DIR))
}

/// Retrieves the path to the `~/.fluvio/bin` directory in the host system.
pub fn fluvio_binaries_path() -> Result<PathBuf> {
    Ok(fluvio_path()?.join("bin"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fvm_workdir_path() {
        let fvm_path = fvm_workdir_path().expect("Failed to get fvm path");
        let home = home_dir().expect("Failed to get home directory");

        assert_eq!(fvm_path, home.join(FVM_HOME_DIR));
    }

    #[test]
    fn test_fvm_bin_path() {
        let fvm_bin_path = fvm_bin_path().expect("Failed to get fvm bin path");
        let fvm_path = fvm_workdir_path().expect("Failed to get fvm path");

        assert_eq!(fvm_bin_path, fvm_path.join("bin").join(FVM_BINARY_NAME));
    }

    #[test]
    fn test_fvm_versions_path() {
        let fvm_version_path = fvm_versions_path().expect("Failed to get fvm pkgset path");
        let fvm_path = fvm_workdir_path().expect("Failed to get fvm path");

        assert_eq!(fvm_version_path, fvm_path.join(FVM_VERSIONS_DIR));
    }

    #[test]
    fn test_fluvio_path() {
        let fluvio_path = fluvio_path().expect("Failed to get fluvio path");
        let home = home_dir().expect("Failed to get home directory");

        assert_eq!(fluvio_path, home.join(FLUVIO_HOME_DIR));
    }

    #[test]
    fn test_fluvio_binaries_path() {
        let fluvio_binaries_path =
            fluvio_binaries_path().expect("Failed to get fluvio binaries path");
        let fluvio_path = fluvio_path().expect("Failed to get fluvio path");

        assert_eq!(fluvio_binaries_path, fluvio_path.join("bin"));
    }
}
