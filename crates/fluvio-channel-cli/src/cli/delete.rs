use std::path::PathBuf;

use clap::{Parser, CommandFactory};
use tracing::debug;
use anyhow::{anyhow, Result};

use fluvio_channel::FluvioChannelConfig;

#[derive(Debug, Parser, Clone, Eq, PartialEq)]
pub struct DeleteOpt {
    /// Path to alternate channel config
    #[clap(long)]
    config: Option<PathBuf>,
    /// Name of release channel
    channel: Option<String>,
    // binary-path
    // extension-path
    // image_tag_strategy
    /// Display this help message
    #[clap(short, long)]
    help: bool,
}

impl DeleteOpt {
    pub async fn process(&self) -> Result<()> {
        if self.help {
            let _ = DeleteOpt::command().print_help();
            println!();
            return Ok(());
        }
        // Open config file

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

            if let Ok(mut load_config) = FluvioChannelConfig::from_file(cli_config_path) {
                debug!("Loaded channel config");
                if let Ok(Some(channel_info)) = load_config.remove_channel(channel_name.clone()) {
                    load_config.save()?;

                    debug!(
                        "Delete binary and extensions dir for channel \"{}\"",
                        channel_name
                    );

                    debug!("Deleting: {}", channel_info.get_binary_path().display());
                    std::fs::remove_file(channel_info.get_binary_path())?;

                    debug!("Deleting: {}", channel_info.get_extensions_path().display());
                    std::fs::remove_dir_all(channel_info.get_extensions_path())?;

                    println!("Deleted release channel \"{channel_name}\"");
                } else {
                    println!("Release channel \"{channel_name}\" not found");
                };
                Ok(())
            } else {
                println!("No channel config found");
                Ok(())
            }
        } else {
            println!("No channel name provided");
            let _ = DeleteOpt::command().print_help();
            println!();
            Err(anyhow!(""))
        }
    }
}
