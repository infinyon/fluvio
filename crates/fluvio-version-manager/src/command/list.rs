//! Show Intalled Versions Command
//!
//! The `show` command is responsible of listing all the installed Fluvio Versions

use anyhow::{Result, anyhow};
use clap::Parser;
use colored::Colorize;
use comfy_table::{Table, Row};

use fluvio_hub_util::fvm::Channel;

use crate::common::manifest::VersionManifest;
use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::version_directory::VersionDirectory;
use crate::common::workdir::fvm_versions_path;

#[derive(Debug, Parser)]
pub struct ListOpt {
    /// List included artifacts for this installed version if available
    #[arg(index = 1)]
    channel: Option<Channel>,
}

impl ListOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let versions_path = fvm_versions_path()?;

        if !versions_path.exists() {
            notify.warn("Cannot list installed versions because there are no versions installed");
            notify.help(format!(
                "You can install a Fluvio version using the command {}",
                "fvm install".bold()
            ));

            return Err(anyhow!("No versions installed"));
        }

        if let Some(channel) = &self.channel {
            let (manifests, _) = VersionDirectory::scan_versions_manifests(versions_path, None)?;
            if let Some(manifest) = manifests.iter().find(|m| m.channel == *channel) {
                if let Some(contents) = &manifest.contents {
                    if matches!(manifest.channel, Channel::Tag(_)) {
                        println!(
                            "Artifacts in version {}",
                            manifest.version.to_string().bold()
                        );
                    } else {
                        println!(
                            "Artifacts in channel {} version {}",
                            manifest.channel.to_string().bold(),
                            manifest.version.to_string().bold()
                        );
                    }

                    for art in contents {
                        println!("{}@{}", art.name, art.version);
                    }

                    return Ok(());
                }

                let suggested_command = format!("{} {}", "fvm install", channel);

                notify.help(format!(
                    "No version contents recorded. You can upadate included artifact details by reinstalling this version. {}",
                    suggested_command.bold(),
                ));
            }

            return Ok(());
        }

        let settings = Settings::open()?;
        let (manifests, maybe_active) =
            VersionDirectory::scan_versions_manifests(versions_path, settings.channel)?;

        if manifests.is_empty() && maybe_active.is_none() {
            notify.warn("No installed versions found");
            notify.help(format!(
                "You can install a Fluvio version using the command {}",
                "fvm install".bold()
            ));

            return Ok(());
        }

        Self::render_table(manifests, maybe_active);
        Ok(())
    }

    /// Creates a `Table` and renders it to the terminal.
    fn render_table(manifests: Vec<VersionManifest>, maybe_active: Option<VersionManifest>) {
        let mut table = Table::new();

        table.set_header(Row::from([" ", "CHANNEL", "VERSION"]));

        if let Some(active) = maybe_active {
            table.add_row(Row::from([
                "✓".to_string(),
                active.channel.to_string(),
                active.version.to_string(),
            ]));
        }

        let mut sorted_manifests = manifests;
        sorted_manifests.sort_by(|a, b| b.channel.cmp(&a.channel));

        for manifest in sorted_manifests {
            table.add_row(Row::from([
                " ".to_string(),
                manifest.channel.to_string(),
                manifest.version.to_string(),
            ]));
        }

        table.load_preset(comfy_table::presets::NOTHING);

        println!("{}", table);
    }
}
