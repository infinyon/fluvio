use crate::Result;
use crate::channel::FluvioChannelConfig;
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

#[derive(Debug, StructOpt, Clone, PartialEq)]
pub struct ListOpt {
    #[structopt(long)]
    config: Option<PathBuf>,
}

impl ListOpt {
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
        let config = if let Ok(load_config) = FluvioChannelConfig::from_file(cli_config_path) {
            debug!("Loaded channel config");
            load_config
        } else {
            debug!("No channel config found, using default");
            FluvioChannelConfig::default()
        };

        let current_channel = config.current_channel();

        for c in config.list_channels() {
            if current_channel == c {
                println!("* {}", &c);
            } else {
                println!("  {}", &c);
            }
        }

        Ok(())
    }
}
