use std::fs::File;
use std::io::{Result, Read};

use sha2::{Digest, Sha256};

/// The mode to set for executable files on Unix platforms.
const EXECUTABLE_MODE: u32 = 0o700;

/// Adds `u+rwx` permissions to the specified file.
///
/// # Platform Compatibility
///
/// This is a no-op on non-Unix platforms.
#[cfg(unix)]
pub fn set_executable_mode(file: &mut File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    // Add u+rwx mode to the existing file permissions, leaving others unchanged
    let mut permissions = file.metadata()?.permissions();
    let mut mode = permissions.mode();

    mode |= EXECUTABLE_MODE;
    permissions.set_mode(mode);
    file.set_permissions(permissions)?;

    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_file: &mut File) -> Result<()> {
    Ok(())
}

/// Generates the SHA256 checksum of the specified file.
///
/// Internally clones the `File` as it needs to read through the entire file.
/// This could be expensive in certain environments of constrained resources.
pub fn shasum256(file: &File) -> Result<String> {
    let meta = file.metadata()?;
    let mut file = file.clone();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; meta.len() as usize];

    file.read_exact(&mut buffer)?;
    hasher.update(buffer);

    let output = hasher.finalize();
    Ok(hex::encode(output))
}
