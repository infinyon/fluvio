//! Show Intalled Versions Command
//!
//! The `show` command is responsible of listing all the installed Fluvio Versions

use std::path::PathBuf;

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
pub struct ShowOpt;

impl ShowOpt {
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

        let settings = Settings::open()?;
        let (manifests, maybe_active) =
            Self::scan_versions_manifests(versions_path, settings.channel)?;

        Self::render_table(manifests, maybe_active);

        Ok(())
    }

    /// Retrieves the sorted list of installed versions [`VersionManifest`]
    /// instances. In parallel, it also retrieves the active version if any.
    ///
    /// If theres any Active Version, the [`VersionManifest`] is not included
    /// as part of the first tuple value, instead it is returned as the second
    /// tuple value.
    fn scan_versions_manifests(
        versions_path: PathBuf,
        maybe_active: Option<Channel>,
    ) -> Result<(Vec<VersionManifest>, Option<VersionManifest>)> {
        let dir_entries = versions_path.read_dir()?;
        let mut manifests: Vec<VersionManifest> = Vec::new();
        let mut active_version: Option<VersionManifest> = None;

        for entry in dir_entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let version_dir = VersionDirectory::open(path.to_path_buf())?;

                if let Some(ref active_channel) = maybe_active {
                    if *active_channel == version_dir.manifest.channel {
                        active_version = Some(version_dir.manifest);
                        continue;
                    }
                }

                manifests.push(version_dir.manifest);
            }
        }

        Ok((manifests, active_version))
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

#[cfg(test)]
mod test {
    use std::path::Path;

    use fs_extra::dir::{copy as copy_dir, CopyOptions};
    use tempfile::TempDir;

    use super::*;

    /// Creates a Temporal Directory with the contents of `fixtures/version`
    /// directory
    fn make_versions_directory() -> Result<TempDir> {
        let fixtures_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("version");
        let tmp = TempDir::new()?;

        copy_dir(fixtures_path, tmp.path(), &CopyOptions::default())?;

        Ok(tmp)
    }

    #[test]
    fn lists_versions_in_dir() {
        let tmpdir = make_versions_directory().unwrap();
        let (manifests, _) =
            ShowOpt::scan_versions_manifests(tmpdir.path().to_path_buf().join("version"), None)
                .unwrap();
        let expected_versions = vec!["0.10.14", "0.10.15", "stable"];

        for ver in expected_versions {
            assert!(
                manifests.iter().any(|m| m.channel.to_string() == ver),
                "version {ver} not found",
            );
        }
    }

    #[test]
    fn determines_active_version() {
        let tmpdir = make_versions_directory().unwrap();
        let (_, active) = ShowOpt::scan_versions_manifests(
            tmpdir.path().to_path_buf().join("version"),
            Some(Channel::Stable),
        )
        .unwrap();

        assert_eq!(active.unwrap().channel, Channel::Stable);
    }
}
