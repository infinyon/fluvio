//! The `settings.toml` is in charge of keeping track of the active version
//! through the default key, which holds the name of the directory under
//! `~/.fvm/pkgset/default/versions` for the desired default version.

use std::fs::read_to_string;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Result;
use crate::install::fvm_path;

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("Failed to open settings file. {0}")]
    OpenFile(std::io::Error),
    #[error("Failed to parse settings file. {0}")]
    Parse(#[from] toml::de::Error),
    #[error("Unable to find settings file in {0}")]
    NotFound(PathBuf),
}

/// FVM Settings file (`settings.toml`)
#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub active: String,
}

impl Settings {
    pub fn open() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            let contents =
                read_to_string(settings_path).map_err(|err| SettingsError::OpenFile(err))?;
            let settings: Settings =
                toml::from_str(&contents).map_err(|err| SettingsError::Parse(err))?;

            return Ok(settings);
        }

        Err(crate::Error::SettingsError(SettingsError::NotFound(settings_path)))
    }

    pub fn create() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            let contents =
                read_to_string(settings_path).map_err(|err| SettingsError::OpenFile(err))?;
            let settings: Settings =
                toml::from_str(&contents).map_err(|err| SettingsError::Parse(err))?;

            return Ok(settings);
        }

        Err(crate::Error::SettingsError(SettingsError::NotFound(settings_path)))
    }

    /// Retrieves the path to the `settings.toml` file for this host
    fn settings_file_path() -> Result<PathBuf> {
        let fvm_path = fvm_path()?;
        let settings_path = fvm_path.join("settings.toml");

        Ok(settings_path)
    }
}
