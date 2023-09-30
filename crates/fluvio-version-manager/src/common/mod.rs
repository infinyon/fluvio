pub mod notify;
pub mod settings;
pub mod workdir;

use std::path::PathBuf;

use color_eyre::eyre::{Error, Result};

/// Wrapper on `dirs::home_dir` which returns `anyhow::Error` instead of `Option`.
pub(super) fn home_dir() -> Result<PathBuf> {
    if let Some(home_dir) = dirs::home_dir() {
        return Ok(home_dir);
    }

    Err(Error::msg("Failed to resolve home directory"))
}
