use crate::Result;
use crate::channel::FluvioChannelConfig;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

#[derive(Debug, StructOpt, Clone, PartialEq)]
pub struct DeleteOpt {
    #[structopt(long)]
    config: Option<PathBuf>,
    channel: String,
    // binary-path
    // extension-path
    // image_tag_strategy
}

impl DeleteOpt {
    pub async fn process(self) -> Result<()> {
        // Open config file

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

        if let Ok(mut load_config) = FluvioChannelConfig::from_file(cli_config_path) {
            debug!("Loaded channel config");
            let _ = if let Ok(Some(channel_info)) = load_config.remove_channel(self.channel.clone())
            {
                load_config.save()?;

                debug!(
                    "Delete binary and extensions dir for channel \"{}\"",
                    self.channel
                );

                debug!("Deleting: {}", channel_info.get_binary_path().display());
                std::fs::remove_file(channel_info.get_binary_path())?;

                debug!("Deleting: {}", channel_info.get_extensions_path().display());
                std::fs::remove_dir_all(channel_info.get_extensions_path())?;

                println!("Deleted release channel \"{}\"", self.channel);
            } else {
                println!("Release channel \"{}\" not found", self.channel);
            };
            Ok(())
        } else {
            println!("No channel config found");
            Ok(())
        }
    }
}
