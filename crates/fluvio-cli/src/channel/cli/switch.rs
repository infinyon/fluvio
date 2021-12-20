use crate::{Result, CliError};
use crate::channel::{FluvioChannelConfig, FluvioChannelInfo, initial_install_fluvio_bin_latest};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

#[derive(Debug, StructOpt, Clone, PartialEq)]
pub struct SwitchOpt {
    channel: String,
    #[structopt(long)]
    config: Option<PathBuf>,
}

impl SwitchOpt {
    pub async fn process(self) -> Result<()> {
        // Load in the config file
        // Parse with the CLI Config parser

        debug!("Looking for channel config");

        let cli_config_path = if let Some(path) = self.config {
            debug!("Using provided channel config path");
            path
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

        debug!("Looking for channel {} in config", &self.channel);
        let mut new_config = if config.get_channel(&self.channel).is_some() {
            //println!("Found");
            debug!("Found channel info for {}", &self.channel);
            config
        } else {
            debug!("channel not found, checking if one of default channels");
            // We support 3 default channels. Only add them to config when we switch to them the first time

            let mut new_config_channel = config;

            let channel = if &self.channel == "stable" {
                FluvioChannelInfo::stable_channel()
            } else if &self.channel == "latest" {
                FluvioChannelInfo::latest_channel()
            } else if &self.channel == "dev" {
                FluvioChannelInfo::dev_channel()
            } else {
                return Err(CliError::Other("Channel not found".to_string()));
            };

            debug!(
                "Updating channel config with new channel {} config {:#?}",
                self.channel.clone(),
                &channel
            );
            new_config_channel.insert_channel(self.channel.clone(), channel)?;
            new_config_channel.set_current_channel(self.channel.clone())?;
            new_config_channel.save()?;

            // Install the latest fluvio binary the first time we switch
            if &self.channel == "latest"
                && !FluvioChannelInfo::latest_channel()
                    .get_binary_path()
                    .exists()
            {
                debug!("Installing latest channel binary for first time");
                initial_install_fluvio_bin_latest(&new_config_channel).await?;
            }

            new_config_channel
        };

        new_config.set_current_channel(self.channel.clone())?;
        new_config.save()?;

        debug!("channel config: {:?}", self.channel);

        Ok(())
    }
}
