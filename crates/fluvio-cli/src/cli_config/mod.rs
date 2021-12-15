use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs::{File, create_dir_all, read_to_string};
use std::io::{ErrorKind, Error as IoError, Write};
use std::str::FromStr;
use crate::Result;
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
            debug!("Home directory not found. Using current dir");
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
    fn developer_channel() -> Self {
        Self {
            binary_location: PathBuf::default(),
            extensions: PathBuf::default(),
        }
        //let mut binary_location;

        //let mut extensions;
        //if let Some(home_dir) = home_dir() {
        //    binary_location = home_dir.clone();
        //    extensions = home_dir.clone();

        //    binary_location.push(CLI_CONFIG_PATH);
        //    binary_location.push("bin");
        //    binary_location.push("fluvio-dev");

        //    extensions.push(CLI_CONFIG_PATH);
        //    extensions.push("extensions-dev");
        //} else {
        //    // No home directory
        //    binary_location = PathBuf::default();
        //    extensions = PathBuf::default();

        //    binary_location.push(CLI_CONFIG_PATH);
        //    binary_location.push("bin");
        //    binary_location.push("fluvio-dev");

        //    extensions.push(CLI_CONFIG_PATH);
        //    extensions.push("extensions-dev");
        //}

        //Self {
        //    binary_location,
        //    extensions,
        //}
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

#[derive(Debug, StructOpt)]
pub struct CliConfigOpt {
    #[structopt(long, possible_values = &CliChannelName::variants(), case_insensitive = true)]
    set_channel: Option<CliChannelName>,
    #[structopt(long)]
    config: Option<PathBuf>,
}

impl CliConfigOpt {
    pub fn process(self) -> Result<()> {
        // Load in the config file
        // Parse with the CLI Config parser

        let cli_config_path = if let Some(path) = self.config {
            path
        } else {
            FluvioChannelConfig::default_config_location()
        };

        // Open file

        let config = if let Ok(config) = FluvioChannelConfig::from_file(cli_config_path) {
            config
        } else {
            FluvioChannelConfig::default()
        };

        let config = if let Some(channel) = self.set_channel {
            // Change channel
            // Check if config knows about the channel first
            let mut new_config = if config
                .config
                .channel
                .get(&channel.to_string())
                .is_some()
            {
                //println!("Found");
                config
            } else {
                // This is really just a matter of changing channels the first time
                // So we'll add the both channel info into the channel file
                let mut new_config_channel = config;

                new_config_channel
                    .config
                    .channel
                    .insert("stable".to_string(), FluvioChannelInfo::stable_channel());
                new_config_channel
                    .config
                    .channel
                    .insert("latest".to_string(), FluvioChannelInfo::latest_channel());

                // I'm not sure we need this?
                new_config_channel
                    .config
                    .channel
                    .insert("dev".to_string(), FluvioChannelInfo::developer_channel());
                new_config_channel
            };

            // Change active channel
            let c = channel.to_string().to_lowercase();
            new_config.config.current_channel = c.clone();

            println!("Channel set to: {} ", c);

            // Write the changes to config
            new_config.save()?;
            new_config
        } else {
            // Do nothing
            config
        };

        debug!("channel config: {:?}", config);

        Ok(())
    }
}
