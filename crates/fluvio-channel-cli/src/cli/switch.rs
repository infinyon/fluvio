use std::path::PathBuf;

use clap::{Parser, CommandFactory};
use tracing::debug;
use anyhow::{anyhow, Result};

use fluvio_channel::{
    FluvioChannelConfig, FluvioChannelInfo, FluvioBinVersion, DEV_CHANNEL_NAME,
    STABLE_CHANNEL_NAME, LATEST_CHANNEL_NAME,
};

use crate::install_channel_fluvio_bin;

#[derive(Debug, Clone, Parser, Eq, PartialEq)]
pub struct SwitchOpt {
    /// Name of release channel
    channel: Option<String>,

    /// Path to alternate channel config
    #[clap(long)]
    config: Option<PathBuf>,

    /// Display this help message
    #[clap(short, long)]
    help: bool,
}

impl SwitchOpt {
    pub async fn process(&self) -> Result<()> {
        if self.help {
            let _ = SwitchOpt::command().print_help();
            println!();
            return Ok(());
        }

        // Load in the config file
        // Parse with the CLI Config parser

        debug!("Looking for channel config");

        let cli_config_path = if let Some(path) = &self.config {
            debug!("Using provided channel config path");
            path.to_path_buf()
        } else {
            debug!("Using default channel config path");
            FluvioChannelConfig::default_config_location()
        };

        // Open file

        let config = if let Ok(load_config) = FluvioChannelConfig::from_file(cli_config_path) {
            debug!("Loaded channel config");
            load_config
        } else {
            debug!("No channel config found, using default");
            FluvioChannelConfig::default()
        };

        // Change channel

        // See if the channel file exists, if not, write a default config

        if let Some(channel_name) = &self.channel {
            debug!("Looking for channel {} in config", &channel_name);
            let mut new_config = if config.get_channel(channel_name).is_some() {
                //println!("Found");
                debug!("Found channel info for {}", &channel_name);
                config
            } else {
                debug!("channel not found, checking if one of default channels");
                // We support 3 default channels. Only add them to config when we switch to them the first time

                let mut new_config_channel = config;

                let channel = if channel_name == STABLE_CHANNEL_NAME {
                    FluvioChannelInfo::stable_channel()
                } else if channel_name == LATEST_CHANNEL_NAME {
                    FluvioChannelInfo::latest_channel()
                } else if channel_name == DEV_CHANNEL_NAME {
                    FluvioChannelInfo::dev_channel()
                } else {
                    return Err(anyhow!(
                        "Channel not found in channel config. (Did you create it first?)"
                    ));
                };

                debug!(
                    "Updating channel config with new channel {} config {:#?}",
                    channel_name.clone(),
                    &channel
                );
                new_config_channel.insert_channel(channel_name.clone(), channel)?;
                new_config_channel.set_current_channel(channel_name.clone())?;
                new_config_channel.save()?;

                // Install the latest fluvio binary the first time we switch
                if channel_name == STABLE_CHANNEL_NAME
                    && !FluvioChannelInfo::stable_channel()
                        .get_binary_path()
                        .exists()
                {
                    debug!("Installing stable channel binary for first time");
                    let version = FluvioBinVersion::parse(channel_name)?;
                    install_channel_fluvio_bin(channel_name.clone(), &new_config_channel, version)
                        .await?;
                }
                if channel_name == LATEST_CHANNEL_NAME
                    && !FluvioChannelInfo::latest_channel()
                        .get_binary_path()
                        .exists()
                {
                    debug!("Installing latest channel binary for first time");
                    let version = FluvioBinVersion::parse(channel_name)?;
                    install_channel_fluvio_bin(channel_name.clone(), &new_config_channel, version)
                        .await?;
                }

                new_config_channel
            };

            new_config.set_current_channel(channel_name.clone())?;
            new_config.save()?;

            debug!("channel config: {:?}", channel_name.clone());
            println!("Switched to release channel \"{channel_name}\"");

            Ok(())
        } else {
            println!("No channel name provided");
            let _ = SwitchOpt::command().print_help();
            println!();
            Err(anyhow!(""))
        }
    }
}
