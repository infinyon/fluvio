use std::fs::File;
use std::io::Error as IoError;

/// The mode to set for executable files on Unix platforms.
const EXECUTABLE_MODE: u32 = 0o700;

/// Adds `u+rwx` permissions to the specified file.
///
/// # Platform Compatibility
///
/// This is a no-op on non-Unix platforms.
#[cfg(unix)]
pub fn set_executable_mode(file: &mut File) -> std::result::Result<(), IoError> {
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
fn make_executable(_file: &mut File) -> std::result::Result<(), IoError> {
    Ok(())
}
