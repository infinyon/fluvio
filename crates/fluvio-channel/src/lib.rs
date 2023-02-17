use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::fs::{File, create_dir_all, read_to_string};
use std::io::{ErrorKind, Error as IoError, Write};

use clap::{Parser, ValueEnum};
use dirs::home_dir;
use serde::{Serialize, Deserialize};
use tracing::debug;
use semver::Version;
use cfg_if::cfg_if;
use thiserror::Error;
use anyhow::{anyhow, Result};

use fluvio_types::defaults::CLI_CONFIG_PATH;

// Default channels
pub const DEV_CHANNEL_NAME: &str = "dev";
pub const STABLE_CHANNEL_NAME: &str = "stable";
pub const LATEST_CHANNEL_NAME: &str = "latest";

// Environment vars for Channels
pub const FLUVIO_RELEASE_CHANNEL: &str = "FLUVIO_RELEASE_CHANNEL";
pub const FLUVIO_EXTENSIONS_DIR: &str = "FLUVIO_EXTENSIONS_DIR";
pub const FLUVIO_IMAGE_TAG_STRATEGY: &str = "FLUVIO_IMAGE_TAG_STRATEGY";

#[derive(Error, Debug)]
pub enum ChannelConfigError {
    #[error(transparent)]
    ConfigFileError(#[from] IoError),
    #[error("Failed to deserialize Fluvio config")]
    TomlError(#[from] toml::de::Error),
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FluvioChannelConfig {
    pub path: PathBuf,
    pub config: ChannelConfig,
}

impl Default for FluvioChannelConfig {
    fn default() -> Self {
        Self {
            path: Self::default_config_location(),
            config: ChannelConfig::default(),
        }
    }
}

impl FluvioChannelConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            ..Default::default()
        }
    }

    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, ChannelConfigError> {
        let path_ref = path.as_ref();
        let file_str: String =
            read_to_string(path_ref).map_err(ChannelConfigError::ConfigFileError)?;
        let channel_config: ChannelConfig =
            toml::from_str(&file_str).map_err(ChannelConfigError::TomlError)?;

        let config = FluvioChannelConfig {
            path: path_ref.to_path_buf(),
            config: channel_config,
        };

        debug!("Config from file: {:#?}", &config);

        Ok(config)
    }

    pub fn get_channel(&self, channel_name: &str) -> Option<FluvioChannelInfo> {
        self.config
            .channel
            .get(channel_name)
            .map(|channel| channel.to_owned())
    }

    pub fn list_channels(&self) -> Vec<String> {
        let mut channels: Vec<String> = self.config.channel.keys().map(|k| k.to_string()).collect();
        channels.sort();
        channels
    }

    pub fn insert_channel(
        &mut self,
        channel_name: String,
        channel: FluvioChannelInfo,
    ) -> Result<()> {
        let _ = self.config.channel.insert(channel_name, channel);

        Ok(())
    }

    pub fn remove_channel(&mut self, channel_name: String) -> Result<Option<FluvioChannelInfo>> {
        let info = self.config.channel.remove(&channel_name);
        Ok(info)
    }

    pub fn save(&self) -> Result<(), ChannelConfigError> {
        create_dir_all(self.path.parent().unwrap()).map_err(ChannelConfigError::ConfigFileError)?;
        self.config
            .save_to(&self.path)
            .map_err(ChannelConfigError::ConfigFileError)?;
        Ok(())
    }

    // TODO: Look at the install mod for info like this
    pub fn default_config_location() -> PathBuf {
        if let Some(mut config_location) = home_dir() {
            config_location.push(CLI_CONFIG_PATH);
            config_location.push("channel");
            config_location
        } else {
            debug!("Home directory not found. Using current dir with relative path");
            PathBuf::from(".")
        }
    }

    // Returns true if a channel config file exists in the configured location
    pub fn exists(path: &Path) -> bool {
        path.exists()
    }

    pub fn current_channel(&self) -> String {
        self.config.current_channel.clone()
    }

    pub fn set_current_channel(&mut self, channel_name: String) -> Result<()> {
        self.config.current_channel = channel_name;
        Ok(())
    }

    pub fn current_exe(&self) -> Option<PathBuf> {
        self.config
            .channel
            .get(&self.current_channel())
            .map(|channel_info| channel_info.binary_location.clone())
    }

    pub fn current_extensions(&self) -> Option<PathBuf> {
        self.config
            .channel
            .get(&self.current_channel())
            .map(|channel_info| channel_info.extensions.clone())
    }

    pub fn config(&self) -> ChannelConfig {
        self.config.clone()
    }

    pub fn set_path(&mut self, path: PathBuf) -> Result<()> {
        self.path = path;
        Ok(())
    }

    pub fn update_config(&mut self, config: ChannelConfig) -> Result<()> {
        self.config = config;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub current_channel: String,
    pub channel: HashMap<String, FluvioChannelInfo>,
}

impl ChannelConfig {
    pub fn current_channel(&self) -> String {
        self.current_channel.clone()
    }

    pub fn channel(&self) -> HashMap<String, FluvioChannelInfo> {
        self.channel.clone()
    }
}

#[derive(Debug, Parser, ValueEnum, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[clap(rename_all = "kebab-case")]
pub enum ImageTagStrategy {
    Version,
    VersionGit,
    Git,
}

impl Default for ImageTagStrategy {
    fn default() -> Self {
        Self::Version
    }
}

impl Display for ImageTagStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageTagStrategy::Version => f.write_str("version"),
            ImageTagStrategy::VersionGit => f.write_str("version-git"),
            ImageTagStrategy::Git => f.write_str("git"),
        }
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FluvioChannelInfo {
    pub binary_location: PathBuf,
    pub extensions: PathBuf,
    pub image_tag_strategy: ImageTagStrategy,
}

// TODO: Clean up all the duplication mess
// Look at the install module to re-use stuff
impl FluvioChannelInfo {
    pub fn new(
        binary_location: PathBuf,
        extensions: PathBuf,
        image_tag_strategy: ImageTagStrategy,
    ) -> Self {
        Self {
            binary_location,
            extensions,
            image_tag_strategy,
        }
    }

    pub fn new_channel(channel_name: &str, image_tag_strategy: ImageTagStrategy) -> Self {
        let extensions_dir_name = if channel_name == "stable" {
            "extensions".to_string()
        } else {
            format!("extensions-{channel_name}")
        };

        // This is to handle windows binaries, which should end in `.exe`
        cfg_if! {
            if #[cfg(not(target_os = "windows"))] {
                let fluvio_bin_name = format!("fluvio-{channel_name}");
            } else {
                let fluvio_bin_name = format!("fluvio-{channel_name}.exe");
            }
        }

        let (binary_location, extensions) = if let Some(home_dir) = home_dir() {
            let binary_location = PathBuf::from(format!(
                "{}/{}/bin/{}",
                home_dir.display(),
                CLI_CONFIG_PATH,
                fluvio_bin_name
            ));
            let extensions = PathBuf::from(format!(
                "{}/{}/{}",
                home_dir.display(),
                CLI_CONFIG_PATH,
                extensions_dir_name
            ));

            (binary_location, extensions)
        } else {
            // No home directory
            let binary_location = PathBuf::from(format!("{CLI_CONFIG_PATH}/bin/{fluvio_bin_name}"));
            let extensions = PathBuf::from(format!("{CLI_CONFIG_PATH}/{extensions_dir_name}"));

            (binary_location, extensions)
        };

        Self::new(binary_location, extensions, image_tag_strategy)
    }

    pub fn set_binary_path(&mut self, path: PathBuf) -> Result<()> {
        self.binary_location = path;
        Ok(())
    }

    pub fn set_extensions_path(&mut self, path: PathBuf) -> Result<()> {
        self.extensions = path;
        Ok(())
    }

    pub fn set_tag_strategy(&mut self, strategy: ImageTagStrategy) -> Result<()> {
        self.image_tag_strategy = strategy;
        Ok(())
    }

    pub fn get_binary_path(&self) -> PathBuf {
        self.binary_location.clone()
    }

    pub fn get_extensions_path(&self) -> PathBuf {
        self.extensions.clone()
    }

    pub fn get_image_tag_strategy(&self) -> ImageTagStrategy {
        self.image_tag_strategy.clone()
    }

    pub fn stable_channel() -> Self {
        Self::new_channel("stable", ImageTagStrategy::Version)
    }

    pub fn latest_channel() -> Self {
        Self::new_channel("latest", ImageTagStrategy::VersionGit)
    }

    pub fn dev_channel() -> Self {
        cfg_if! {
            if #[cfg(not(target_os = "windows"))] {
                let fluvio_bin_name = "fluvio".to_string();
            } else {
                let fluvio_bin_name = "fluvio.exe".to_string();
            }
        }
        // fluvio binary is expected to be in same dir as the current binary
        if let Ok(mut exe) = std::env::current_exe() {
            exe.set_file_name(fluvio_bin_name);

            Self {
                binary_location: exe,
                extensions: PathBuf::from("extensions"),
                image_tag_strategy: ImageTagStrategy::Git,
            }
        } else {
            // Otherwise assume to be in same dir as current working directory
            Self {
                binary_location: PathBuf::from(fluvio_bin_name),
                extensions: PathBuf::from("extensions"),
                image_tag_strategy: ImageTagStrategy::Git,
            }
        }
    }
}

impl ChannelConfig {
    // save to file
    pub fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError> {
        let path_ref = path.as_ref();
        debug!("saving config: {:#?} to: {:#?}", self, path_ref);
        let toml = toml::to_string(self)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("{err}")))?;

        let mut file = File::create(path_ref)?;
        file.write_all(toml.as_bytes())?;
        file.sync_all()
    }
}

pub enum FluvioBinVersion {
    Dev,
    Stable,
    Latest,
    Tag(Version),
}

impl FluvioBinVersion {
    pub fn parse(version_str: &str) -> Result<Self> {
        if version_str.to_lowercase() == "stable" {
            Ok(Self::Stable)
        } else if version_str.to_lowercase() == "latest" {
            Ok(Self::Latest)
        } else {
            let semver = if let Ok(semver) = Version::parse(version_str) {
                semver
            } else {
                return Err(anyhow!("Unable to resolve version"));
            };
            Ok(Self::Tag(semver))
        }
    }
}

// Check if we're running Fluvio in the location our installer places binaries
// This is used to decide whether to use channel config
pub fn is_fluvio_bin_in_std_dir(fluvio_bin: &Path) -> bool {
    let fluvio_home_dir = if let Some(mut fluvio_home) = home_dir() {
        fluvio_home.push(CLI_CONFIG_PATH);
        fluvio_home
    } else {
        return false;
    };

    // Verify if fluvio_bin is in the same directory as {home_dir/CLI_CONFIG_PATH}
    fluvio_bin.starts_with(fluvio_home_dir)
}

/// Determine if provided channel name is a pinned version channel
pub fn is_pinned_version_channel(channel_name: &str) -> bool {
    !matches!(channel_name, "stable" | "latest" | "dev")
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_is_pinned_version_channel() {
        let test_cases = [
            ("stable", false),
            ("latest", false),
            ("dev", false),
            ("X.Y.Z", true),
            ("f.l.u.v.i.o", true),
        ];

        for case in test_cases {
            assert_eq!(is_pinned_version_channel(case.0), case.1);
        }
    }
}
