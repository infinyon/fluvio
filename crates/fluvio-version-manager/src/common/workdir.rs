//! The `Workdir` is the directory used by Fluvio Version Manager (FVM) to
//! store its files and binaries.

use std::path::PathBuf;

use anyhow::Result;

use super::home_dir;

/// Home Directory for the Fluvio Version Manager (FVM) CLI
pub const FVM_HOME_DIR: &str = ".fvm";

/// FVM Binary Name
pub const FVM_BINARY_NAME: &str = "fvm";

/// FVM Packages Set Directory Name
pub const FVM_PACKAGES_SET_DIR: &str = "pkgset";

/// Retrieves the path to the `~/.fvm` directory in the host system
pub fn fvm_workdir_path() -> Result<PathBuf> {
    let fvm_path = home_dir()?.join(FVM_HOME_DIR);

    Ok(fvm_path)
}

/// Retrieves the path to the `~/.fvm/bin/fvm` directory in the host system
pub fn fvm_bin_path() -> Result<PathBuf> {
    let fvm_workdir_path = fvm_workdir_path()?;
    let fvm_binary_path = fvm_workdir_path.join("bin").join(FVM_BINARY_NAME);

    Ok(fvm_binary_path)
}

/// Retrieves the path to the `~/.fvm/pkgset` directory in the host system
pub fn fvm_pkgset_path() -> Result<PathBuf> {
    let fvm_workdir_path = fvm_workdir_path()?;
    let fvm_pkget_path = fvm_workdir_path.join(FVM_PACKAGES_SET_DIR);

    Ok(fvm_pkget_path)
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
    fn test_fvm_pkgset_path() {
        let fvm_pkgset_path = fvm_pkgset_path().expect("Failed to get fvm pkgset path");
        let fvm_path = fvm_workdir_path().expect("Failed to get fvm path");

        assert_eq!(fvm_pkgset_path, fvm_path.join(FVM_PACKAGES_SET_DIR));
    }
}
