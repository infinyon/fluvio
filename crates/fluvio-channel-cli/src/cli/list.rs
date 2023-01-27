use std::path::PathBuf;

use clap::{Parser, CommandFactory};
use tracing::debug;
use anyhow::{Result};

use fluvio_channel::FluvioChannelConfig;

#[derive(Debug, Clone, Parser, Eq, PartialEq)]
pub struct ListOpt {
    /// Path to alternate channel config
    #[clap(long)]
    config: Option<PathBuf>,

    /// Display this help message
    #[clap(short, long)]
    help: bool,
}

impl ListOpt {
    pub async fn process(&self) -> Result<()> {
        if self.help {
            let _ = ListOpt::command().print_help();
            println!();
            return Ok(());
        }

        // Open config file

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
