pub mod create;
pub mod delete;
pub mod list;
pub mod switch;

use fluvio_channel::{FluvioChannelConfig, DEV_CHANNEL_NAME};
use tracing::debug;

pub fn current_channel() -> String {
    debug!("Looking for channel config");

    let cli_config_path = FluvioChannelConfig::default_config_location();

    // Check if exe is in the standard fluvio home dir
    // If it isn't, assume dev channel
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if !cli_config_path.starts_with(dir) {
                return DEV_CHANNEL_NAME.to_string();
            }
        }
    }

    // Open file

    let config = if let Ok(load_config) = FluvioChannelConfig::from_file(cli_config_path) {
        debug!("Loaded channel config");
        load_config
    } else {
        debug!("No channel config found, using default");
        FluvioChannelConfig::default()
    };

    config.current_channel()
}
