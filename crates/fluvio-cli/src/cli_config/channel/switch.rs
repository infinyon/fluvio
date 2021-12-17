use crate::Result;
use std::path::PathBuf;
use structopt::StructOpt;
use super::{CliChannelName, FluvioChannelConfig, FluvioChannelInfo, initial_install_fluvio_bin_latest};
use tracing::debug;

#[derive(Debug, StructOpt)]
pub struct SwitchOpt {
    #[structopt(possible_values = &CliChannelName::variants(), case_insensitive = true)]
    channel: CliChannelName,
    #[structopt(long)]
    config: Option<PathBuf>,
}
impl SwitchOpt {
    pub async fn process(self) -> Result<()> {
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

        let config = {
            // Change channel

            // See if the channel file exists, if not, write a default config

            // If channel == CliChannelName::Dev, then we only want to change the current channel
            //

            let mut new_config = if config
                .config
                .channel
                .get(&self.channel.to_string())
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

                new_config_channel
            };

            // Change active channel
            let c = self.channel.to_string().to_lowercase();
            new_config.config.current_channel = c.clone();

            println!("Channel set to {} ", c);

            // Write the changes to config
            new_config.save()?;

            // We should get the list of extensions from stable, and download it for latest

            if new_config.current_channel() == CliChannelName::Latest {
                // Only if the binary for the channel doesn't exist, download it
                // The user needs to run `fluvio update` to update their `fluvio-latest`
                let fluvio_latest_bin_exists =
                    if let Some(c) = new_config.config.channel.get("latest") {
                        c.clone().binary_location.exists()
                    } else {
                        FluvioChannelInfo::latest_channel().binary_location.exists()
                    };

                if !fluvio_latest_bin_exists {
                    initial_install_fluvio_bin_latest(&new_config).await?;
                }
            }

            new_config
        };

        debug!("channel config: {:?}", config);

        Ok(())
    }
}
