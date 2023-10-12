//! Version Switching Command
//!
//! The `switch` command is responsible of changing the active Fluvio Version

use std::fs::{create_dir_all, read_dir, copy};
use std::path::{PathBuf, Path};

use anyhow::Result;
use clap::Parser;
use colored::Colorize;

use fluvio_hub_util::fvm::Channel;

use crate::GlobalOptions;
use crate::common::manifest::{PACKAGE_SET_MANIFEST_FILENAME, VersionManifest};
use crate::common::settings::Settings;
use crate::common::workdir::{fvm_versions_path, fluvio_binaries_path};
use crate::common::notify::Notify;

/// Represents the contents of a version directory (`~/.fvm/versions/<version>`)
/// where binaries and the manifest are stored.
///
/// The `VersionDirectory` represents the contents of a version directory
/// in 2 parts:
///
/// - `contents`: Every file in the directory except for the manifest
/// - `manifest`: The manifest file
struct VersionDirectory {
    contents: Vec<PathBuf>,
    manifest: PathBuf,
}

impl VersionDirectory {
    /// Opens a directory and returns a `VersionDirectory` build from scanning it
    fn open(path: PathBuf) -> Result<Self> {
        let mut dir = Self {
            contents: vec![],
            manifest: PathBuf::new(),
        };

        for entry in read_dir(path)? {
            let entry = entry?;

            if entry.metadata()?.is_dir() {
                continue;
            }

            let entry_path = entry.path();

            if entry_path.file_name().ok_or(anyhow::anyhow!(
                "Failed to get filename from path: {}",
                entry_path.display()
            ))? == PACKAGE_SET_MANIFEST_FILENAME
            {
                dir.manifest = entry_path;
            } else {
                dir.contents.push(entry_path);
            }
        }

        if dir.manifest == PathBuf::new() {
            return Err(anyhow::anyhow!(
                "Failed to find manifest file in version directory"
            ));
        }

        Ok(dir)
    }

    /// Copies `contents` files into the `target` directory
    fn copy_contents(&self, target: impl AsRef<Path>) -> Result<()> {
        for entry in &self.contents {
            let filename = entry.file_name().ok_or(anyhow::anyhow!(
                "Failed to get filename from path: {}",
                entry.display()
            ))?;
            let target_path = target.as_ref().join(filename);

            copy(entry, &target_path)?;
            tracing::info!(?target_path, "Copied binary");
        }

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct SwitchOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Version to install
    #[arg(index = 1, default_value_t = Channel::Stable)]
    version: Channel,
}

impl SwitchOpt {
    pub async fn process(&self) -> Result<()> {
        let versions_path = fvm_versions_path()?;

        // If the user runs this command before installing any versions, we
        // should notify them and exit early.
        if !versions_path.exists() {
            self.notify_warn("No local Fluvio versions found.");
            self.notify_help(format!(
                "Try installing a version with {}, and then retry this command.",
                "fvm install".bold()
            ));

            return Ok(());
        }

        let pkgset_path = versions_path.join(self.version.to_string());

        // If the package is not available locally, we should notify the user
        // and exit early.
        if !pkgset_path.exists() {
            self.notify_warn(format!(
                "Fluvio version {} is not installed",
                self.version.to_string().bold()
            ));

            let help = format!("fvm install {}", self.version);

            self.notify_help(format!(
                "Install the desired version using {}, and then retry this command.",
                help.bold()
            ));

            return Ok(());
        }

        let fluvio_bin_dir = fluvio_binaries_path()?;

        if !fluvio_bin_dir.exists() {
            tracing::info!(?fluvio_bin_dir, "Creating fluvio binaries directory");
            create_dir_all(&fluvio_bin_dir)?;
        }

        // Opens the version directory and then copies the contents (but the manifest)
        // to the Fluvio Binaries directory
        let version_dir = VersionDirectory::open(pkgset_path)?;
        version_dir.copy_contents(&fluvio_bin_dir)?;

        // Update settings file to keep track of active Fluvio Version
        let manifest = VersionManifest::open(version_dir.manifest)?;
        let mut settings = Settings::open()?;
        settings.update_version(&manifest.channel, &manifest.version)?;

        self.notify_done(format!(
            "Now using Fluvio version {}",
            self.version.to_string().bold(),
        ));

        Ok(())
    }
}

impl Notify for SwitchOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn opens_directory_as_version_dir() {
        let tmpdir = tempfile::tempdir().unwrap();
        let manifest = tmpdir.path().join(PACKAGE_SET_MANIFEST_FILENAME);
        let random_file = tmpdir.path().join("random_file");

        File::create(&manifest).unwrap();
        File::create(&random_file).unwrap();

        let entries = VersionDirectory::open(tmpdir.path().to_path_buf()).unwrap();

        assert_eq!(entries.contents.len(), 1);
        assert_eq!(entries.contents[0], random_file);
        assert_eq!(entries.manifest, manifest);
    }

    #[test]
    fn fails_to_open_directory_without_manifest() {
        let tmpdir = tempfile::tempdir().unwrap();
        let result = VersionDirectory::open(tmpdir.path().to_path_buf());

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Failed to find manifest file in version directory"
        );
    }

    #[test]
    fn copies_contents_to_target_directory() {
        let tmpdir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let manifest = tmpdir.path().join(PACKAGE_SET_MANIFEST_FILENAME);
        let random_file = tmpdir.path().join("random_file");

        File::create(manifest).unwrap();
        File::create(&random_file).unwrap();

        let entries = VersionDirectory::open(tmpdir.path().to_path_buf()).unwrap();
        entries.copy_contents(&target_dir).unwrap();

        let target_entries = read_dir(target_dir.path()).unwrap();
        let target_entries: Vec<PathBuf> =
            target_entries.map(|entry| entry.unwrap().path()).collect();

        assert_eq!(target_entries.len(), 1);
        assert_eq!(
            target_entries.get(0).unwrap(),
            &target_dir.path().join(random_file.file_name().unwrap())
        );
    }
}
