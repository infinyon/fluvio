use std::path::PathBuf;
use std::fs::{create_dir, rename};

use anyhow::Result;
use tempfile::TempDir;

use fluvio_hub_util::fvm::{PackageSet, Download, Channel};

use super::manifest::VersionManifest;
use super::notify::Notify;
use super::version_directory::VersionDirectory;
use super::workdir::fvm_versions_path;

pub struct VersionInstaller {
    channel: Channel,
    package_set: PackageSet,
    notify: Notify,
}

impl VersionInstaller {
    pub fn new(channel: Channel, package_set: PackageSet, notify: Notify) -> Self {
        Self {
            channel,
            package_set,
            notify,
        }
    }

    pub async fn install(&self) -> Result<()> {
        // The `tmp_dir` must be dropped after copying the binaries to the
        // destination directory. By dropping `tmp_dir` the directory will be
        // deleted from the filesystem.
        let tmp_dir = TempDir::new()?;

        for (idx, artf) in self.package_set.artifacts.iter().enumerate() {
            self.notify.info(format!(
                "Downloading ({}/{}): {}@{}",
                idx + 1,
                self.package_set.artifacts.len(),
                artf.name,
                artf.version
            ));

            let artf_path = artf.download(tmp_dir.path().to_path_buf()).await?;
            Self::set_executable_mode(artf_path)?;
        }

        let version_path = self.store_artifacts(&tmp_dir, &self.package_set).await?;
        let manifest =
            VersionManifest::new(self.channel.to_owned(), self.package_set.pkgset.clone());

        manifest.write(&version_path)?;
        self.notify.done(format!(
            "Installed fluvio version {}",
            self.package_set.pkgset
        ));

        let version_dir = VersionDirectory::open(version_path)?;

        version_dir.set_active()?;

        self.notify
            .done(format!("Now using fluvio version {}", manifest.version));

        Ok(())
    }

    /// Allocates artifacts in the FVM `versions` directory for future use.
    /// Returns the path to the allocated version directory.
    async fn store_artifacts(
        &self,
        tmp_dir: &TempDir,
        package_set: &PackageSet,
    ) -> Result<PathBuf> {
        let version_path = fvm_versions_path()?.join(&self.channel.to_string());

        if !version_path.exists() {
            create_dir(&version_path)?;
        }

        for artif in package_set.artifacts.iter() {
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
        use std::{os::unix::fs::PermissionsExt, fs::File};

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
    use std::fs::File;

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

        VersionInstaller::set_executable_mode(path.clone()).unwrap();

        let file = File::open(&path).unwrap();
        let meta = file.metadata().unwrap();
        let perm = meta.permissions();
        let is_executable = perm.mode() & 0o111 != 0;

        assert!(is_executable, "should be executable");
    }
}
