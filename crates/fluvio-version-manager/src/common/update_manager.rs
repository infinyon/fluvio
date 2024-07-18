use std::fs::File;
use std::io::copy;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use semver::Version;
use tempfile::TempDir;

use fluvio_hub_util::sha256_digest;
use ureq::OrAnyStatus;

use crate::common::executable::{remove_fvm_binary_if_exists, set_executable_mode};

use super::notify::Notify;
use super::workdir::fvm_bin_path;
use super::TARGET;

/// Updates Manager for the Fluvio Version Manager
pub struct UpdateManager {
    notify: Notify,
}

impl UpdateManager {
    pub fn new(notify: &Notify) -> Self {
        Self {
            notify: notify.to_owned(),
        }
    }

    async fn fetch_checksum_for_version(&self, version: &Version) -> Result<String> {
        let checksum_url = format!(
            "https://packages.fluvio.io/v1/packages/fluvio/fvm/{}/{}/fvm.sha256",
            version, TARGET
        );
        let request = ureq::get(&checksum_url);

        let response = request.call().or_any_status().context(format!(
            "Failed to fetch checksum for fvm@{} from {}",
            version, checksum_url
        ))?;

        let checksum = response.into_string()?;
        Ok(checksum)
    }

    pub async fn update(&self, version: &Version) -> Result<()> {
        self.notify.info(format!("Downloading fvm@{}", version));
        let (_tmp_dir, new_fvm_bin) = self.download(version).await?;

        self.notify.info(format!("Installing fvm@{}", version));
        self.install(&new_fvm_bin).await?;
        self.notify
            .done(format!("Installed fvm@{} with success", version));

        Ok(())
    }

    /// Downloads Fluvio Version Manager binary into a temporary directory
    async fn download(&self, version: &Version) -> Result<(TempDir, PathBuf)> {
        let tmp_dir = TempDir::new()?;
        let download_url = format!(
            "https://packages.fluvio.io/v1/packages/fluvio/fvm/{}/{}/fvm",
            version, TARGET
        );
        let request = ureq::get(&download_url);

        tracing::info!(download_url, "Downloading FVM");

        let response = request.call().or_any_status().context(format!(
            "Failed to download fvm@{} from {}",
            version, download_url
        ))?;

        let out_path = tmp_dir.path().join("fvm");
        let mut file = File::create(&out_path)?;

        copy(&mut response.into_reader(), &mut file)?;
        self.checksum(version, &out_path).await?;
        set_executable_mode(&out_path)?;

        Ok((tmp_dir, out_path))
    }

    /// Verifies downloaded FVM binary checksums against the upstream checksums
    async fn checksum(&self, version: &Version, path: &PathBuf) -> Result<()> {
        let local_file_shasum = sha256_digest(path)?;
        let upstream_shasum = self.fetch_checksum_for_version(version).await?;

        if local_file_shasum != upstream_shasum {
            bail!(
                "Checksum mismatch for fvm@{}: local={}, upstream={}",
                version,
                local_file_shasum,
                upstream_shasum
            );
        }

        Ok(())
    }

    async fn install(&self, new_fvm_bin: &PathBuf) -> Result<()> {
        let old_fvm_bin = fvm_bin_path()?;

        if !new_fvm_bin.exists() {
            tracing::warn!(?new_fvm_bin, "New fvm binary not found. Aborting update.");
            bail!("Failed to update FVM due to missing binary");
        }

        remove_fvm_binary_if_exists()?;

        tracing::warn!(src=?new_fvm_bin, dst=?old_fvm_bin , "Copying new fvm binary");
        std::fs::copy(new_fvm_bin, &old_fvm_bin)?;

        Ok(())
    }
}
