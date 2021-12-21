use crate::Result;
use crate::channel::{
    FluvioChannelConfig, FluvioChannelInfo, ImageTagStrategy, install_channel_fluvio_bin,
    FluvioBinVersion,
};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;
use dirs::home_dir;
use fluvio_types::defaults::CLI_CONFIG_PATH;

#[derive(Debug, StructOpt, Clone, PartialEq)]
pub struct CreateOpt {
    #[structopt(long)]
    config: Option<PathBuf>,
    channel: String,
    binary_path: Option<PathBuf>,
    extensions_path: Option<PathBuf>,
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

        // Configure where to save binaries
        let home = if let Some(mut config_location) = home_dir() {
            config_location.push(CLI_CONFIG_PATH);
            config_location
        } else {
            debug!("Home directory not found. Using current dir with relative path");
            PathBuf::from(".")
        };

        let binary_path = if let Some(binary_path) = self.binary_path {
            binary_path
        } else {
            // Default to ~/.fluvio/bin/fluvio-<channel>
            let mut p = home.clone();
            p.push("bin");
            p.push(format!("fluvio-{}", self.channel));
            p
        };

        let extensions_path = if let Some(extensions_path) = self.extensions_path {
            extensions_path
        } else {
            // Default to ~/.fluvio/bin/extensions-<channel>
            let mut p = home.clone();
            p.push("bin");
            p.push(format!("extensions-{}", self.channel));
            p
        };

        let image_tag_strategy = if let Some(strategy) = self.image_tag_strategy {
            strategy
        } else {
            ImageTagStrategy::Version
        };

        let mut new_channel_info = FluvioChannelInfo::default();
        new_channel_info.set_binary_path(binary_path.clone())?;
        new_channel_info.set_extensions_path(extensions_path)?;
        new_channel_info.set_tag_strategy(image_tag_strategy)?;

        config.insert_channel(self.channel.clone(), new_channel_info)?;
        config.save()?;

        // TODO: We should also try to install the binary if it doesn't exist in the binary path
        if !binary_path.exists() {
            let version = FluvioBinVersion::parse(&self.channel)?;

            install_channel_fluvio_bin(self.channel, &config, version).await?;
        }

        Ok(())
    }
}
