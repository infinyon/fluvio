use std::fs::{read_to_string, write};
use std::path::PathBuf;

use color_eyre::eyre::{Error, Result};
use serde::{Deserialize, Serialize};
use semver::Version;

use fluvio_hub_util::fvm::Channel;

use super::workdir::fvm_workdir_path;

pub const SETTINGS_TOML_FILENAME: &str = "settings.toml";

/// The `settings.toml` is in charge of keeping track of the active version
/// through the default key, which holds the name of the directory under
/// `~/.fvm/pkgset/default/versions` for the desired default version.
#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    /// The active `channel` for the Fluvio Installation
    pub channel: Option<Channel>,
    /// The specific version in use
    pub version: Option<Version>,
}

impl Settings {
    /// Opens the `settings.toml` file and parses it into a `Settings` struct
    pub fn open() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            let contents = read_to_string(settings_path)?;
            let settings: Settings = toml::from_str(&contents)?;

            return Ok(settings);
        }

        Err(Error::msg(format!(
            "Settings file not found on {}",
            settings_path.display()
        )))
    }

    /// Used to create an empty `settings.toml` file. This is used when the user
    /// installs FVM but no version is set yet.
    pub fn init() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            return Err(Error::msg(format!(
                "Settings already exists on {}",
                settings_path.display()
            )));
        }

        let initial = Self {
            channel: None,
            version: None,
        };

        initial.save()?;
        tracing::debug!(?settings_path, "Created settings file with success");

        Ok(initial)
    }

    /// Determines if the `settings.toml` file has an active version
    pub fn version_parts(&self) -> (Option<Channel>, Option<Version>) {
        (self.channel.clone(), self.version.clone())
    }

    pub fn active_channel(&self) -> Option<Channel> {
        self.channel.clone()
    }

    pub fn active_version(&self) -> Option<Version> {
        self.version.clone()
    }

    /// Sets the active version in the `settings.toml` file
    pub fn set_active(&mut self, channel: Channel, version: Version) -> Result<()> {
        self.channel = Some(channel);
        self.version = Some(version);
        self.save()?;

        Ok(())
    }

    /// Saves the `settings.toml` file to disk, overwriting the previous version
    fn save(&self) -> Result<()> {
        let settings_path = Self::settings_file_path()?;
        let settings_str = toml::to_string(&self)?;

        write(settings_path, settings_str)?;

        Ok(())
    }

    /// Retrieves the path to the `settings.toml` file for this host
    fn settings_file_path() -> Result<PathBuf> {
        let fvm_path = fvm_workdir_path()?;
        let settings_path = fvm_path.join(SETTINGS_TOML_FILENAME);

        Ok(settings_path)
    }
}
