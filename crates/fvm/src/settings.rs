//! The `settings.toml` is in charge of keeping track of the active version
//! through the default key, which holds the name of the directory under
//! `~/.fvm/pkgset/default/versions` for the desired default version.

use std::fs::{read_to_string, write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use fluvio_hub_util::fvm::Channel;

use crate::Result;
use crate::install::fvm_path;

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("Failed to open settings file. {0}")]
    OpenFile(std::io::Error),
    #[error("Failed to parse settings file. {0}")]
    Parse(#[from] toml::de::Error),
    #[error("Failed to serialize settings file. {0}")]
    Serialize(String),
    #[error("Unable to find settings file in {0}")]
    NotFound(PathBuf),
    #[error("Failed to write file to disk. {0}")]
    WriteFile(std::io::Error),
    #[error("Settings file already exists in {0}")]
    AlreadyExists(PathBuf),
}

/// FVM Settings file (`settings.toml`)
#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub active: Channel,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            active: Channel::Stable,
        }
    }
}

impl Settings {
    pub fn open() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            let contents = read_to_string(settings_path).map_err(SettingsError::OpenFile)?;
            let settings: Settings = toml::from_str(&contents).map_err(SettingsError::Parse)?;

            return Ok(settings);
        }

        Err(SettingsError::NotFound(settings_path).into())
    }

    pub fn create() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            return Err(SettingsError::AlreadyExists(settings_path).into());
        }

        let initial = Self::default();

        initial.save()?;
        Ok(initial)
    }

    /// Sets the active version in the `settings.toml` file
    pub fn set_active(&mut self, channel: Channel) -> Result<()> {
        self.active = channel;
        self.save()?;

        Ok(())
    }

    fn save(&self) -> Result<()> {
        let settings_path = Self::settings_file_path()?;
        let settings_str =
            toml::to_string(&self).map_err(|err| SettingsError::Serialize(err.to_string()))?;

        write(settings_path, settings_str).map_err(SettingsError::WriteFile)?;

        Ok(())
    }

    /// Retrieves the path to the `settings.toml` file for this host
    fn settings_file_path() -> Result<PathBuf> {
        let fvm_path = fvm_path()?;
        let settings_path = fvm_path.join("settings.toml");

        Ok(settings_path)
    }
}
