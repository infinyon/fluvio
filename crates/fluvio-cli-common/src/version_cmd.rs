use anyhow::Result;
use clap::Args;
use current_platform::CURRENT_PLATFORM;
use crate::FLUVIO_RELEASE_CHANNEL;
use sha2::{Digest, Sha256};
use fluvio_version::{GIT_HASH, VERSION};

/// Display version information
#[derive(Debug, Args)]
pub struct BasicVersionCmd;

impl BasicVersionCmd {
    /// Display basic information about the current fluvio installation
    ///
    /// The following information is displayed:
    /// - Release channel, if available;
    /// - CLI version;
    /// - Platform arch;
    /// - CLI SHA256, if available;
    /// - channel frontend SHA256, if available;
    /// - Git hash, if available;
    /// - OS details, if available;
    pub fn process(self, cli_name: &str) -> Result<()> {
        if let Ok(channel) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
            println!("Release Channel: {channel}");
        }

        println!("{cli_name} CLI: {VERSION}");
        println!("{cli_name} CLI Arch: {CURRENT_PLATFORM}");

        if let Some(sha) = self.format_cli_sha() {
            println!("{cli_name} CLI SHA256: {}", sha);
        }

        println!("Git Commit: {GIT_HASH}");

        if let Some(info) = os_info() {
            println!("OS Details: {info}");
        }

        Ok(())
    }

    /// Read CLI and compute its sha256
    fn format_cli_sha(&self) -> Option<String> {
        let path = std::env::current_exe().ok()?;
        let bin = std::fs::read(path).ok()?;
        let mut hasher = Sha256::new();
        hasher.update(bin);
        let bin_sha256 = hasher.finalize();
        Some(format!("{:x}", &bin_sha256))
    }
}

fn os_info() -> Option<String> {
    use sysinfo::SystemExt;
    let sys = sysinfo::System::new_all();

    let info = format!(
        "{} {} (kernel {})",
        sys.name()?,
        sys.os_version()?,
        sys.kernel_version()?,
    );

    Some(info)
}
