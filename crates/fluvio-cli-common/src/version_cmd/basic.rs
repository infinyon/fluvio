use anyhow::Result;
use clap::Args;

use crate::FLUVIO_RELEASE_CHANNEL;

use super::{FluvioVersionPrinter, os_info};

const VERSION: &str = include_str!("../../../../VERSION");

/// Display version information for local Fluvio CLIs using this build of
/// Fluvio details such as `VERSION`, `GIT_HASH`, and `FLUVIO_RELEASE_CHANNEL`.
#[derive(Debug, Args)]
pub struct BasicVersionCmd {
    #[cfg(feature = "serde")]
    #[clap(short, long)]
    /// Output in JSON format
    pub json: bool,
}

impl BasicVersionCmd {
    /// Display basic information about the current fluvio installation
    ///
    /// The following information is displayed:
    /// - Release channel, if available;
    /// - CLI version;
    /// - Platform arch;
    /// - CLI SHA256, if available;
    /// - Git hash, if available;
    /// - OS details, if available;
    pub fn process(self, cli_name: &str) -> Result<()> {
        let mut fluvio_version_printer = FluvioVersionPrinter::new(cli_name, VERSION);

        if let Ok(channel) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
            fluvio_version_printer.append_extra("Release Channel", channel);
        }

        if let Ok(git_hash) = std::env::var("GIT_HASH") {
            fluvio_version_printer.append_extra("Git Commit", git_hash);
        }

        if let Some(info) = os_info() {
            fluvio_version_printer.append_extra("OS Details", info);
        }

        #[cfg(feature = "serde")]
        {
            if self.json {
                println!("{}", fluvio_version_printer.to_json_pretty()?);
                return Ok(());
            }
        }

        println!("{}", fluvio_version_printer);

        Ok(())
    }
}
