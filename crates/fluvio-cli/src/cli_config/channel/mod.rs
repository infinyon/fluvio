use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs::{File, create_dir_all, read_to_string};
use std::io::{ErrorKind, Error as IoError, Write};
use std::str::FromStr;
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

mod switch;
use switch::SwitchOpt;
mod list;

#[derive(Debug, StructOpt, Default)]
pub struct ChannelOpt {
    #[structopt(subcommand)]
    cmd: Option<ChannelCmd>,
}

impl ChannelOpt {
    pub async fn process(self) -> Result<()> {
        match self.cmd {
            Some(cmd) => cmd.process().await?,
            None => {
                CurrentOpt::default().process().await?
            },
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub enum ChannelCmd {
    /// Change the CLI Release channel
    Switch(SwitchOpt),
    /// List the CLI Release channel
    List,
}

impl ChannelCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            ChannelCmd::Switch(switch) => switch.process().await?,
            ChannelCmd::List => {
                for c in CliChannelName::variants() {
                    println!("{}", c.to_lowercase())
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt, Default)]
struct CurrentOpt {
    #[structopt(long)]
    config: Option<PathBuf>,
}

impl CurrentOpt {
    pub async fn process(self) -> Result<()> {

        // Look for channel config
        let channel_config_path = if let Some(path) = self.config {
            path
        } else {
            FluvioChannelConfig::default_config_location()
        };

        let current_channel = if let Ok(config) = FluvioChannelConfig::from_file(channel_config_path) {
            Some(config.current_channel())
        } else {
            None
        };

        if let Some(channel) = current_channel {
            println!("Current channel: {}", channel.to_string().to_lowercase())
        } else {
            println!("No channel set")
        }

        Ok(())
    }
}

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

    pub fn current_channel(&self) -> CliChannelName {
        CliChannelName::from_str(&self.config.current_channel).unwrap_or_default()
    }

    pub fn current_exe(&self) -> Option<PathBuf> {
        self.config
            .channel
            .get(&self.current_channel().to_string().to_lowercase())
            .map(|channel_info| channel_info.binary_location.clone())
    }

    pub fn current_extensions(&self) -> Option<PathBuf> {
        self.config
            .channel
            .get(&self.current_channel().to_string().to_lowercase())
            .map(|channel_info| channel_info.extensions.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfig {
    current_channel: String,
    channel: HashMap<String, FluvioChannelInfo>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        let default_channel = "stable".to_string();
        let default_channel_info = FluvioChannelInfo::default();

        let mut channel_map = HashMap::new();
        channel_map.insert(default_channel.clone(), default_channel_info);

        Self {
            current_channel: default_channel,
            channel: channel_map,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluvioChannelInfo {
    binary_location: PathBuf,
    extensions: PathBuf,
}

impl Default for FluvioChannelInfo {
    fn default() -> Self {
        if let Some(home_dir) = home_dir() {
            let mut binary_location = home_dir.clone();
            binary_location.push(CLI_CONFIG_PATH);
            binary_location.push("bin");
            binary_location.push("fluvio");

            let mut extensions = home_dir;
            extensions.push(CLI_CONFIG_PATH);
            extensions.push("extensions");

            Self {
                binary_location,
                extensions,
            }
        } else {
            Self {
                binary_location: PathBuf::default(),
                extensions: PathBuf::default(),
            }
        }
    }
}

// TODO: Clean up all the duplication mess
// Look at the install module to re-use stuff
impl FluvioChannelInfo {
    fn stable_channel() -> Self {
        let mut binary_location;

        let mut extensions;
        if let Some(home_dir) = home_dir() {
            binary_location = home_dir.clone();
            extensions = home_dir;

            binary_location.push(CLI_CONFIG_PATH);
            binary_location.push("bin");
            binary_location.push("fluvio");

            extensions.push(CLI_CONFIG_PATH);
            extensions.push("extensions");
        } else {
            // No home directory
            binary_location = PathBuf::default();
            extensions = PathBuf::default();

            binary_location.push(CLI_CONFIG_PATH);
            binary_location.push("bin");
            binary_location.push("fluvio");

            extensions.push(CLI_CONFIG_PATH);
            extensions.push("extensions");
        }

        Self {
            binary_location,
            extensions,
        }
    }

    fn latest_channel() -> Self {
        let mut binary_location;

        let mut extensions;
        if let Some(home_dir) = home_dir() {
            binary_location = home_dir.clone();
            extensions = home_dir;

            binary_location.push(CLI_CONFIG_PATH);
            binary_location.push("bin");
            binary_location.push("fluvio-latest");

            extensions.push(CLI_CONFIG_PATH);
            extensions.push("extensions-latest");
        } else {
            // No home directory
            binary_location = PathBuf::default();
            extensions = PathBuf::default();

            binary_location.push(CLI_CONFIG_PATH);
            binary_location.push("bin");
            binary_location.push("fluvio-latest");

            extensions.push(CLI_CONFIG_PATH);
            extensions.push("extensions-latest");
        }

        Self {
            binary_location,
            extensions,
        }
    }
}

impl ChannelConfig {
    // save to file
    fn save_to<T: AsRef<Path>>(&self, path: T) -> Result<(), IoError> {
        let path_ref = path.as_ref();
        debug!("saving config: {:#?} to: {:#?}", self, path_ref);
        let toml =
            toml::to_vec(self).map_err(|err| IoError::new(ErrorKind::Other, format!("{}", err)))?;

        let mut file = File::create(path_ref)?;
        file.write_all(&toml)
    }
}

arg_enum! {
    #[derive(Debug, StructOpt, Clone, PartialEq)]
    #[structopt(rename_all = "kebab-case")]
    pub enum CliChannelName {
        Stable,
        Latest,
        Dev,
    }
}

impl Default for CliChannelName {
    fn default() -> Self {
        Self::Stable
    }
}

// The first time we switch to the latest channel, we should install the binary
async fn initial_install_fluvio_bin_latest(channel_config: &FluvioChannelConfig) -> Result<()> {
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
