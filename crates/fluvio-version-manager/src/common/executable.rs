//! Executable file utilities.

use std::fs::remove_file;

use anyhow::Result;

use super::workdir::fvm_bin_path;

/// Removes FVM Binary if present in the FVM home directory
pub fn remove_fvm_binary_if_exists() -> Result<()> {
    let fvm_binary_path = fvm_bin_path()?;

    if fvm_binary_path.exists() {
        remove_file(fvm_binary_path)?;
    }

    Ok(())
}

/// Sets the executable mode for the specified file in Unix systems.
/// This is no-op in non-Unix systems.
#[cfg(unix)]
pub fn set_executable_mode(path: &std::path::PathBuf) -> anyhow::Result<()> {
    use std::{fs::File, os::unix::fs::PermissionsExt};

    const EXECUTABLE_MODE: u32 = 0o700;

    // Add u+rwx mode to the existing file permissions, leaving others unchanged
    let file = File::open(path)?;
    let mut permissions = file.metadata()?.permissions();
    let mut mode = permissions.mode();

    mode |= EXECUTABLE_MODE;
    permissions.set_mode(mode);
    file.set_permissions(permissions)?;

    Ok(())
}

/// Setting binary executable mode is a no-op in non-Unix systems.
#[cfg(not(unix))]
pub fn set_executable_mode(path: &std::path::PathBuf) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn sets_unix_execution_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = TempDir::new().unwrap();
        let path = tmpdir.path().join("test");
        let file = File::create(&path).unwrap();
        let meta = file.metadata().unwrap();
        let perm = meta.permissions();
        let is_executable = perm.mode() & 0o111 != 0;

        assert!(!is_executable, "should not be executable");

        set_executable_mode(&path).unwrap();

        let file = File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let perm = meta.permissions();
        let is_executable = perm.mode() & 0o111 != 0;

        assert!(is_executable, "should be executable");
    }
}
