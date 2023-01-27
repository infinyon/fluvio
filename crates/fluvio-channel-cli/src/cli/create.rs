use std::path::PathBuf;

use clap::{Parser, CommandFactory};
use tracing::debug;
use dirs::home_dir;
use anyhow::{anyhow, Result};

use fluvio_types::defaults::CLI_CONFIG_PATH;
use fluvio_channel::{FluvioChannelConfig, FluvioChannelInfo, FluvioBinVersion, ImageTagStrategy};

use crate::install_channel_fluvio_bin;

#[derive(Debug, Clone, Parser, Eq, PartialEq)]
pub struct CreateOpt {
    /// Path to alternate channel config
    #[clap(long)]
    config: Option<PathBuf>,
    /// Name of release channel
    channel: Option<String>,
    /// Path to fluvio binary for channel
    binary_path: Option<PathBuf>,
    /// Path to fluvio extensions directory for channel
    extensions_path: Option<PathBuf>,
    /// Type of Docker image tag strategy (choices: Version, VersionGit, Git)
    #[clap(value_enum)]
    image_tag_strategy: Option<ImageTagStrategy>,
    // Do I need this flag?
    //update: Option<bool>,
    /// Display this help message
    #[clap(short, long)]
    help: bool,
}

impl CreateOpt {
    pub async fn process(&self) -> Result<()> {
        if self.help {
            let _ = CreateOpt::command().print_help();
            println!();
            return Ok(());
        }
        // Load in the config file
        // Parse with the CLI Config parser

        debug!("Looking for channel config");

        if let Some(channel_name) = &self.channel {
            let cli_config_path = if let Some(path) = &self.config {
                debug!("Using provided channel config path");
                path.to_path_buf()
            } else {
                debug!("Using default channel config path");
                FluvioChannelConfig::default_config_location()
            };

            // Open file
            let mut config =
                if let Ok(load_config) = FluvioChannelConfig::from_file(cli_config_path) {
                    debug!("Loaded channel config");
                    load_config
                } else {
                    debug!("No channel config found, using default");
                    FluvioChannelConfig::default()
                };

            // Configure where to save binaries
            let home = if let Some(mut config_location) = home_dir() {
                config_location.push(CLI_CONFIG_PATH);
                config_location
            } else {
                debug!("Home directory not found. Using current dir with relative path");
                PathBuf::from(".")
            };

            let binary_path = if let Some(binary_path) = &self.binary_path {
                binary_path.to_path_buf()
            } else {
                // Default to ~/.fluvio/bin/fluvio-<channel>
                let mut p = home.clone();
                p.push("bin");
                p.push(format!("fluvio-{channel_name}"));
                p
            };

            let extensions_path = if let Some(extensions_path) = &self.extensions_path {
                extensions_path.to_path_buf()
            } else {
                // Default to ~/.fluvio/bin/extensions-<channel>
                let mut p = home.clone();
                p.push("bin");
                p.push(format!("extensions-{channel_name}"));
                p
            };

            let image_tag_strategy = if let Some(strategy) = &self.image_tag_strategy {
                strategy.to_owned()
            } else {
                ImageTagStrategy::Version
            };

            let mut new_channel_info = FluvioChannelInfo::default();
            new_channel_info.set_binary_path(binary_path.clone())?;
            new_channel_info.set_extensions_path(extensions_path)?;
            new_channel_info.set_tag_strategy(image_tag_strategy)?;

            config.insert_channel(channel_name.clone(), new_channel_info)?;

            // We should also try to install the binary if it doesn't exist in the binary path
            if !binary_path.exists() {
                let version = FluvioBinVersion::parse(channel_name)?;
                install_channel_fluvio_bin(channel_name.to_string(), &config, version).await?;
            }

            //fixes issue_2168
            //only save if the version parse is successful
            config.save()?;

            Ok(())
        } else {
            println!("No channel name provided");
            let _ = CreateOpt::command().print_help();
            println!();
            Err(anyhow!(""))
        }
    }
}
