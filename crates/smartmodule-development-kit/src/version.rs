use anyhow::Result;
use clap::Args;
use current_platform::CURRENT_PLATFORM;
use fluvio_cli_common::FLUVIO_RELEASE_CHANNEL;
use sha2::{Digest, Sha256};

const VERSION: &str = include_str!("../../../VERSION");

#[derive(Debug, Args)]
pub struct VersionCmd;

impl VersionCmd {
    pub fn process(self) -> Result<()> {
        if let Ok(channel) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
            println!("Release Channel: {channel}");
        }

        println!("Fluvio CLI: {}", VERSION.trim());
        println!("Fluvio CLI Arch: {CURRENT_PLATFORM}");

        if let Some(sha) = self.format_cli_sha() {
            println!("Fluvio CLI SHA256: {}", sha);
        }

        if let Some(sha) = self.format_frontend_sha() {
            println!("Fluvio channel frontend SHA256: {}", sha);
        }

        println!("Git Commit: {}", env!("GIT_HASH"));

        if let Some(info) = os_info() {
            println!("OS Details: {info}");
        }

        Ok(())
    }

    fn format_frontend_sha(&self) -> Option<String> {
        let fluvio_cli = std::env::current_exe().ok()?;
        let mut fluvio_frontend_path = fluvio_cli;
        fluvio_frontend_path.set_file_name("fluvio");

        let fluvio_cli_bin = std::fs::read(fluvio_frontend_path).ok()?;
        let mut hasher = Sha256::new();
        hasher.update(fluvio_cli_bin);
        let fluvio_cli_bin_sha256 = hasher.finalize();
        Some(format!("{:x}", &fluvio_cli_bin_sha256))
    }

    /// Read CLI and compute its sha256
    fn format_cli_sha(&self) -> Option<String> {
        let path = std::env::current_exe().ok()?;
        let fluvio_bin = std::fs::read(path).ok()?;
        let mut hasher = Sha256::new();
        hasher.update(fluvio_bin);
        let fluvio_bin_sha256 = hasher.finalize();
        Some(format!("{:x}", &fluvio_bin_sha256))
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
