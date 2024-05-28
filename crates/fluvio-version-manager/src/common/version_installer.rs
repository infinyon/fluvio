use std::path::PathBuf;
use std::fs::{copy, create_dir, remove_file, rename};

use anyhow::{anyhow, Result};
use tempfile::TempDir;

use fluvio_hub_util::fvm::{Artifact, Channel, Download, PackageSet};

use super::executable::set_executable_mode;
use super::manifest::{VersionManifest, VersionedArtifact, PACKAGE_SET_MANIFEST_FILENAME};
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
        let tmp_dir = self.download(&self.package_set.artifacts).await?;
        let version_path = self
            .store_artifacts(&tmp_dir, &self.package_set.artifacts)
            .await?;
        let contents = self
            .package_set
            .artifacts
            .iter()
            .map(|art| VersionedArtifact::new(art.name.to_owned(), art.version.to_string()))
            .collect::<Vec<VersionedArtifact>>();
        let manifest = VersionManifest::new(
            self.channel.to_owned(),
            self.package_set.pkgset.clone(),
            contents,
        );

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

    pub async fn update(&self, upstream_artifacts: &[Artifact]) -> Result<()> {
        let tmp_dir = self.download(upstream_artifacts).await?;
        let version_path = self.store_artifacts(&tmp_dir, upstream_artifacts).await?;
        let mut manifest = VersionManifest::open(version_path.join(PACKAGE_SET_MANIFEST_FILENAME))?;
        let mut old_versions: Vec<VersionedArtifact> = Vec::with_capacity(upstream_artifacts.len());

        if let Some(ref contents) = manifest.contents {
            let next: Vec<VersionedArtifact> =
                contents
                    .iter()
                    .fold(Vec::with_capacity(contents.len()), |mut acc, vers_artf| {
                        if let Some(upstr_art) = upstream_artifacts
                            .iter()
                            .find(|art| art.name == vers_artf.name)
                        {
                            acc.push(VersionedArtifact::new(
                                upstr_art.name.to_owned(),
                                upstr_art.version.to_string(),
                            ));
                            old_versions.push(vers_artf.to_owned());
                        } else {
                            acc.push(vers_artf.to_owned());
                        }

                        acc
                    });

            manifest.contents = Some(next);
        }

        manifest.write(&version_path)?;

        old_versions.iter().for_each(|old_var| {
            if let Some(new_var) = upstream_artifacts
                .iter()
                .find(|art| art.name == old_var.name)
            {
                self.notify.info(format!(
                    "Updated {} from {} to {}",
                    old_var.name, old_var.version, new_var.version
                ));
            }
        });

        let version_dir = VersionDirectory::open(version_path)?;
        version_dir.set_active()?;

        Ok(())
    }

    /// Downloads the specified artifacts to the temporary directory and
    /// returns a reference to the temporary directory [`TempDir`].
    ///
    /// The `tmp_dir` must be dropped after copying the binaries to the
    /// destination directory. By dropping [`TempDir`] the directory will be
    /// deleted from the filesystem.
    async fn download(&self, artifacts: &[Artifact]) -> Result<TempDir> {
        let tmp_dir = TempDir::new()?;

        for (idx, artf) in artifacts.iter().enumerate() {
            self.notify.info(format!(
                "Downloading ({}/{}): {}@{}",
                idx + 1,
                artifacts.len(),
                artf.name,
                artf.version
            ));

            let artf_path = artf.download(tmp_dir.path().to_path_buf()).await?;
            set_executable_mode(&artf_path)?;
        }

        Ok(tmp_dir)
    }

    /// Allocates artifacts in the FVM `versions` directory for future use.
    /// Returns the path to the allocated version directory.
    ///
    /// If an artifact with the same name exists in the destination directory,
    /// it will be removed before copying the new artifact.
    async fn store_artifacts(&self, tmp_dir: &TempDir, artifacts: &[Artifact]) -> Result<PathBuf> {
        let version_path = fvm_versions_path()?.join(self.channel.to_string());

        if !version_path.exists() {
            create_dir(&version_path)?;
        }

        for artif in artifacts.iter() {
            let src = tmp_dir.path().join(&artif.name);
            let dst = version_path.join(&artif.name);

            // If the artifact exists in `dst` it should be deleted before copying
            if dst.exists() {
                tracing::debug!(
                    ?dst,
                    "Removing existing artifact in place of upstream artifact"
                );
                remove_file(&dst)?;
            }

            if rename(src.clone(), dst.clone()).is_err() {
                copy(src.clone(), dst.clone()).map_err(|e| {
                    anyhow!(
                        "Error copying artifact {} to {}, {} ",
                        src.display(),
                        dst.display(),
                        e
                    )
                })?;
            }
        }

        Ok(version_path)
    }
}
