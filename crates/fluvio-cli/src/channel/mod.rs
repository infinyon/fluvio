use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs::{File, create_dir_all, read_to_string};
use std::io::{ErrorKind, Error as IoError, Write};
use crate::{Result, CliError};
use crate::install::{install_bin, fetch_latest_version, fetch_package_file, install_println};
use fluvio_index::{PackageId, HttpAgent};
use fluvio::FluvioError;
use fluvio::config::ConfigError;
use fluvio_types::defaults::CLI_CONFIG_PATH;
use structopt::StructOpt;
use structopt::clap::arg_enum;
use dirs::home_dir;
use serde::{Serialize, Deserialize};
use toml;
use thiserror::Error;
use tracing::debug;
use semver::Version;

pub mod cli;

#[derive(Error, Debug)]
pub enum ChannelConfigError {
    #[error(transparent)]
    ConfigFileError(#[from] IoError),
    #[error("Failed to deserialize Fluvio config")]
    TomlError(#[from] toml::de::Error),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluvioChannelConfig {
    path: PathBuf,
    config: ChannelConfig,
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

    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self, FluvioError> {
        let path_ref = path.as_ref();
        let file_str: String = read_to_string(path_ref).map_err(ConfigError::ConfigFileError)?;
        let channel_config: ChannelConfig =
            toml::from_str(&file_str).map_err(ConfigError::TomlError)?;

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

    pub fn save(&self) -> Result<(), FluvioError> {
        create_dir_all(self.path.parent().unwrap()).map_err(ConfigError::ConfigFileError)?;
        self.config
            .save_to(&self.path)
            .map_err(ConfigError::ConfigFileError)?;
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfig {
    current_channel: String,
    channel: HashMap<String, FluvioChannelInfo>,
}

impl ChannelConfig {
    pub fn current_channel(&self) -> String {
        self.current_channel.clone()
    }

    pub fn channel(&self) -> HashMap<String, FluvioChannelInfo> {
        self.channel.clone()
    }
}

arg_enum! {
    #[derive(Debug, StructOpt, Clone, PartialEq, Serialize, Deserialize)]
    #[structopt(rename_all = "kebab-case")]
    pub enum ImageTagStrategy {
        Version,
        VersionGit,
        Git,
    }
}

impl Default for ImageTagStrategy {
    fn default() -> Self {
        Self::Version
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FluvioChannelInfo {
    binary_location: PathBuf,
    extensions: PathBuf,
    image_tag_strategy: ImageTagStrategy,
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
        let (binary_location, extensions) = if let Some(home_dir) = home_dir() {
            let binary_location = PathBuf::from(format!(
                "{}/{}/bin/fluvio-{}",
                home_dir.display(),
                CLI_CONFIG_PATH,
                channel_name
            ));
            let extensions = PathBuf::from(format!(
                "{}/{}/extensions-{}",
                home_dir.display(),
                CLI_CONFIG_PATH,
                channel_name
            ));

            (binary_location, extensions)
        } else {
            // No home directory
            let binary_location =
                PathBuf::from(format!("{}/bin/fluvio-{}", CLI_CONFIG_PATH, channel_name));
            let extensions =
                PathBuf::from(format!("{}/extensions-{}", CLI_CONFIG_PATH, channel_name));

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

    // The dev channel is used by CI
    pub fn dev_channel() -> Self {
        Self {
            binary_location: PathBuf::from("fluvio"),
            extensions: PathBuf::from("extensions"),
            image_tag_strategy: ImageTagStrategy::Git,
        }
    }
}

impl ChannelConfig {
    // save to file
    pub fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError> {
        let path_ref = path.as_ref();
        debug!("saving config: {:#?} to: {:#?}", self, path_ref);
        let toml =
            toml::to_vec(self).map_err(|err| IoError::new(ErrorKind::Other, format!("{}", err)))?;

        let mut file = File::create(path_ref)?;
        file.write_all(&toml)
    }
}

pub enum FluvioBinVersion {
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
                return Err(CliError::Other("Unable to resolve version".to_string()));
            };
            Ok(Self::Tag(semver))
        }
    }
}

// The first time we switch to the latest channel, we should install the binary
pub async fn initial_install_fluvio_bin_latest(channel_config: &FluvioChannelConfig) -> Result<()> {
    let agent = HttpAgent::default();
    let target = fluvio_index::package_target()?;
    let id: PackageId = "fluvio/fluvio".parse()?;
    debug!(%target, %id, "Fluvio CLI updating self:");

    // Find the latest version of this package
    install_println("🎣 Fetching latest version for fluvio...");
    let latest_version = fetch_latest_version(&agent, &id, &target, true).await?;
    let id = id.into_versioned(latest_version.into());

    // Download the package file from the package registry
    install_println(format!(
        "⏳ Downloading Fluvio CLI with latest version: {}...",
        &id.version()
    ));
    let package_result = fetch_package_file(&agent, &id, &target).await;
    let package_file = match package_result {
        Ok(pf) => pf,
        Err(CliError::PackageNotFound {
            version, target, ..
        }) => {
            install_println(format!(
                "❕ Fluvio is not published at version {} for {}, skipping self-update",
                version, target
            ));
            return Ok(());
        }
        Err(other) => return Err(other),
    };
    install_println("🔑 Downloaded and verified package file");

    // Install the update over the current executable
    let fluvio_path = if let Some(c) = channel_config.config.channel.get("latest") {
        c.clone().binary_location
    } else {
        FluvioChannelInfo::latest_channel().binary_location
    };

    install_bin(&fluvio_path, &package_file)?;
    install_println(format!(
        "✅ Successfully updated {}",
        &fluvio_path.display(),
    ));

    Ok(())
}

pub async fn install_channel_fluvio_bin(
    channel_name: String,
    channel_config: &FluvioChannelConfig,
    version: FluvioBinVersion,
) -> Result<()> {
    let agent = HttpAgent::default();
    let target = fluvio_index::package_target()?;
    let id: PackageId = "fluvio/fluvio".parse()?;
    debug!(%target, %id, "Fluvio CLI updating self:");

    // Get the current channel name and info
    let current_channel = channel_name;
    let _channel_info = if let Some(info) = channel_config.get_channel(&current_channel) {
        info
    } else {
        return Err(CliError::Other(
            "Channel info not found in config".to_string(),
        ));
    };

    // Find the latest version of this package
    install_println(format!(
        "🎣 Fetching '{}' channel binary for fluvio...",
        current_channel
    ));

    let install_version = match version {
        FluvioBinVersion::Stable => fetch_latest_version(&agent, &id, &target, false).await?,
        FluvioBinVersion::Latest => fetch_latest_version(&agent, &id, &target, true).await?,
        FluvioBinVersion::Tag(version) => version,
    };

    let id = id.into_versioned(install_version.into());

    // Download the package file from the package registry
    install_println(format!(
        "⏳ Downloading Fluvio CLI with latest version: {}...",
        &id.version()
    ));
    let package_result = fetch_package_file(&agent, &id, &target).await;
    let package_file = match package_result {
        Ok(pf) => pf,
        Err(CliError::PackageNotFound {
            version, target, ..
        }) => {
            install_println(format!(
                "❕ Fluvio is not published at version {} for {}, skipping self-update",
                version, target
            ));
            return Ok(());
        }
        Err(other) => return Err(other),
    };
    install_println("🔑 Downloaded and verified package file");

    // Install the update over the current executable
    let fluvio_path = if let Some(c) = channel_config.config.channel.get(&current_channel) {
        c.clone().binary_location
    } else {
        return Err(CliError::Other(
            "Channel binary location not found".to_string(),
        ));
    };

    install_bin(&fluvio_path, &package_file)?;
    install_println(format!(
        "✅ Successfully updated {}",
        &fluvio_path.display(),
    ));

    Ok(())
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
