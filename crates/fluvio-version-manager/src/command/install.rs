//! Install Command
//!
//! Downloads and stores the sepecific Fluvio Version binaries in the local
//! FVM cache.

use std::fs::{File, create_dir, create_dir_all, rename};

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tempfile::TempDir;
use url::Url;

use fluvio_hub_util::HUB_REMOTE;
use fluvio_hub_util::fvm::{Client, Download, PackageSet, DEFAULT_PKGSET, Channel};

use crate::common::TARGET;
use crate::common::manifest::VersionManifest;
use crate::common::notify::Notify;
use crate::common::workdir::fvm_versions_path;

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct InstallOpt {
    /// Registry used to fetch Fluvio Versions
    #[arg(long, env = "HUB_REGISTRY_URL", default_value = HUB_REMOTE)]
    registry: Url,
    /// Version to install
    #[arg(index = 1, default_value_t = Channel::Stable)]
    version: Channel,
}

impl InstallOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let versions_path = fvm_versions_path()?;

        if !versions_path.exists() {
            tracing::info!(?versions_path, "Creating versions directory");
            create_dir_all(&versions_path)?;
        }

        // The `tmp_dir` must be dropped after copying the binaries to the
        // destination directory. By dropping `tmp_dir` the directory will be
        // deleted from the filesystem.
        let tmp_dir = TempDir::new()?;
        let client = Client::new(self.registry.as_str())?;
        let pkgset = client
            .fetch_package_set(DEFAULT_PKGSET, &self.version, TARGET)
            .await?;

        for (idx, artf) in pkgset.artifacts.iter().enumerate() {
            notify.info(format!(
                "Downloading ({}/{}): {}@{}",
                idx + 1,
                pkgset.artifacts.len(),
                artf.name,
                artf.version
            ));

            let artf_path = artf.download(tmp_dir.path().to_path_buf()).await?;
            Self::set_executable_mode(artf_path)?;
        }

        let version_path = self.store_artifacts(&tmp_dir, &pkgset).await?;

        VersionManifest::new(self.version.to_owned(), pkgset.version.clone())
            .write(version_path)?;
        notify.done(format!("Installed fluvio version {}", self.version));

        Ok(())
    }

    /// Allocates artifacts in the FVM `versions` directory for future use.
    /// Returns the path to the allocated version directory.
    async fn store_artifacts(&self, tmp_dir: &TempDir, pkgset: &PackageSet) -> Result<PathBuf> {
        let version_path = fvm_versions_path()?.join(&self.version.to_string());

        if !version_path.exists() {
            create_dir(&version_path)?;
        }

        for artif in pkgset.artifacts.iter() {
            rename(
                tmp_dir.path().join(&artif.name),
                version_path.join(&artif.name),
            )?;
        }

        Ok(version_path)
    }

    /// Sets the executable mode for the specified file in Unix systems.
    /// This is no-op in non-Unix systems.
    #[cfg(unix)]
    fn set_executable_mode(path: PathBuf) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        const EXECUTABLE_MODE: u32 = 0o700;

        // Add u+rwx mode to the existing file permissions, leaving others unchanged
        let file = File::open(path)?;
        let mut permissions = file.metadata()?.permissions();
        let mut mode = permissions.mode();

        mode |= EXECUTABLE_MODE;
        permissions.set_mode(mode);
        file.set_permissions(permissions)?;

        Ok(())
    }

    /// Setting binary executable mode is a no-op in non-Unix systems.
    #[cfg(not(unix))]
    fn set_executable_mode(path: PathBuf) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sets_unix_execution_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = TempDir::new().unwrap();
        let path = tmpdir.path().join("test");
        let file = File::create(&path).unwrap();
        let meta = file.metadata().unwrap();
        let perm = meta.permissions();
        let is_executable = perm.mode() & 0o111 != 0;

        assert!(!is_executable, "should not be executable");

        InstallOpt::set_executable_mode(path.clone()).unwrap();

        let file = File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let perm = meta.permissions();
        let is_executable = perm.mode() & 0o111 != 0;

        assert!(is_executable, "should be executable");
    }
}
