use crate::Result;
use crate::channel::{
    FluvioChannelConfig, FluvioChannelInfo, ImageTagStrategy, install_channel_fluvio_bin,
    FluvioBinVersion,
};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

#[derive(Debug, StructOpt, Clone, PartialEq)]
pub struct CreateOpt {
    #[structopt(long)]
    config: Option<PathBuf>,
    channel: String,
    image_tag_strategy: Option<ImageTagStrategy>,
    // Do I need this flag?
    //update: Option<bool>,
}

impl CreateOpt {
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
        let mut config = if let Ok(load_config) = FluvioChannelConfig::from_file(cli_config_path) {
            debug!("Loaded channel config");
            load_config
        } else {
            debug!("No channel config found, using default");
            FluvioChannelConfig::default()
        };

        // We can configure the image tag strategy
        let image_tag_strategy = if let Some(strategy) = self.image_tag_strategy {
            strategy
        } else {
            ImageTagStrategy::Version
        };

        let new_channel_info = FluvioChannelInfo::new_channel(&self.channel, image_tag_strategy);

        config.insert_channel(self.channel.clone(), new_channel_info.clone())?;
        config.save()?;

        // We should also try to install the binary if it doesn't exist in the binary path
        if !new_channel_info.binary_location.exists() {
            let version = FluvioBinVersion::parse(&self.channel)?;

            install_channel_fluvio_bin(self.channel, &config, version).await?;
        }

        Ok(())
    }
}
