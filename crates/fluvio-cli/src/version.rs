use sha2::{Digest, Sha256};
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::config::ConfigFile;
use fluvio_extension_common::target::ClusterTarget;
use crate::Result;
use crate::metadata::subcommand_metadata;
use crate::FLUVIO_RELEASE_CHANNEL;

#[derive(Debug, StructOpt)]
pub struct VersionOpt {}

impl VersionOpt {
    pub async fn process(self, target: ClusterTarget) -> Result<()> {
        // IF FLUVIO_RELEASE_CHANNEL defined
        if let Ok(channel_name) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
            self.print("Release Channel", &channel_name);
        };

        self.print("Fluvio CLI", crate::VERSION.trim());

        if let Some(sha) = self.format_cli_sha() {
            self.print("Fluvio CLI SHA256", &sha);
        }
        let platform = self.format_platform_version(target).await;
        self.print("Fluvio Platform", &platform);
        self.print("Git Commit", env!("GIT_HASH"));
        if let Some(os_info) = os_info() {
            self.print("OS Details", &os_info);
        }

        if let Some(metadata) = self.format_subcommand_metadata() {
            println!("=== Plugin Versions ===");
            for (name, version) in metadata {
                self.print_width(&name, &version, 30);
            }
        }

        Ok(())
    }

    fn print(&self, name: &str, version: &str) {
        self.print_width(name, version, 20);
    }

    fn print_width(&self, name: &str, version: &str, width: usize) {
        println!("{:width$} : {}", name, version, width = width);
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

    async fn format_platform_version(&self, target: ClusterTarget) -> String {
        // Attempt to connect to a Fluvio cluster to get platform version
        // Even if we fail to connect, we should not fail the other printouts
        let mut platform_version = String::from("Not available");
        if let Ok(fluvio_config) = target.load() {
            if let Ok(fluvio) = Fluvio::connect_with_config(&fluvio_config).await {
                let version = fluvio.platform_version();
                platform_version = version.to_string();
            }
        }

        let profile_name = ConfigFile::load(None)
            .ok()
            .and_then(|it| {
                it.config()
                    .current_profile_name()
                    .map(|name| name.to_string())
            })
            .map(|name| format!(" ({})", name))
            .unwrap_or_else(|| "".to_string());
        format!("{}{}", platform_version, profile_name)
    }

    fn format_subcommand_metadata(&self) -> Option<Vec<(String, String)>> {
        let metadata = subcommand_metadata().ok()?;
        let mut formats = Vec::new();
        for cmd in metadata {
            let filename = match cmd.path.file_name() {
                Some(f) => f.to_string_lossy().to_string(),
                None => continue,
            };
            let left = format!("{} ({})", cmd.meta.title, filename);
            formats.push((left, cmd.meta.version.to_string()));
        }

        Some(formats)
    }
}

/// Fetch OS information
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
