pub mod executable;
pub mod manifest;
pub mod notify;
pub mod settings;
pub mod update_manager;
pub mod version_directory;
pub mod version_installer;
pub mod workdir;

use std::path::PathBuf;

use anyhow::{Error, Result};

/// The Target Architecture of the current build (e.g. "aarch64-apple-darwin")
///
/// This is injected by Cargo during the build process. Refer to `build.rs` for
/// more information.
pub const TARGET: &str = env!("TARGET");

/// Wrapper on `dirs::home_dir` which returns `anyhow::Error` instead of `Option`.
pub(super) fn home_dir() -> Result<PathBuf> {
    if let Some(home_dir) = dirs::home_dir() {
        Ok(home_dir)
    } else {
        Err(Error::msg("Failed to resolve home directory"))
    }
}
