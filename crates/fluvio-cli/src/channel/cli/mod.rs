pub mod create;
pub mod delete;
pub mod list;
pub mod switch;

use crate::channel::FluvioChannelConfig;
use tracing::debug;

pub fn current_channel() -> String {
    debug!("Looking for channel config");

    let cli_config_path = FluvioChannelConfig::default_config_location();

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
