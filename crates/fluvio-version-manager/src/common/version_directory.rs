use std::fs::{read_dir, copy, create_dir_all, remove_dir_all};
use std::fs::remove_file;

use std::path::PathBuf;

use anyhow::{bail, Result};

use fluvio_hub_util::fvm::{Artifact, Channel, PackageSet};
use semver::Version;

use crate::common::manifest::{PACKAGE_SET_MANIFEST_FILENAME, VersionManifest};
use crate::common::settings::Settings;
use crate::common::workdir::fluvio_binaries_path;
use crate::common::TARGET;

/// Represents the contents of a version directory (`~/.fvm/versions/<version>`)
/// where binaries and the manifest are stored.
///
/// The `VersionDirectory` represents the contents of a version directory
/// in 2 parts:
///
/// - `contents`: Every file in the directory except for the manifest
/// - `manifest`: The manifest file
#[derive(Debug)]
pub struct VersionDirectory {
    /// The Path to this [`VersionDirectory`]
    pub path: PathBuf,
    /// Every file in the directory except for the manifest
    pub contents: Vec<PathBuf>,
    /// Manifest containing metadata on PackageSet
    pub manifest: VersionManifest,
}

impl VersionDirectory {
    /// Opens a directory and returns a `VersionDirectory` build from scanning it
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut contents: Vec<PathBuf> = Vec::new();
        let mut manifest: Option<VersionManifest> = None;

        for entry in read_dir(&path)? {
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
                manifest = Some(VersionManifest::open(&entry_path)?);
            } else {
                contents.push(entry_path);
            }
        }

        let Some(manifest) = manifest else {
            return Err(anyhow::anyhow!(
                "Failed to find manifest file in version directory"
            ));
        };

        Ok(Self {
            path,
            contents,
            manifest,
        })
    }

    /// Deletes this [`VersionDirectory`] directory
    pub fn remove(&self) -> Result<()> {
        if self.path.exists() {
            tracing::info!(?self.path, "Removing version directory");
            remove_dir_all(&self.path)?;
        }

        Ok(())
    }

    /// Sets this version as the active Fluvio Version
    pub fn set_active(&self) -> Result<()> {
        // Verify `~/.fluvio/bin` exists and create it if it doesn't
        let fluvio_bin_dir = fluvio_binaries_path()?;

        if !fluvio_bin_dir.exists() {
            tracing::info!(?fluvio_bin_dir, "Creating fluvio binaries directory");
            create_dir_all(&fluvio_bin_dir)?;
        }

        for entry in &self.contents {
            let filename = entry.file_name().ok_or(anyhow::anyhow!(
                "Failed to get filename from path: {}",
                entry.display()
            ))?;
            let target_path = fluvio_bin_dir.join(filename);

            if let Err(err) = remove_file(&target_path) {
                use tracing::debug;
                match err.kind() {
                    std::io::ErrorKind::NotFound => {}
                    _ => {
                        debug!("ioerr: {}", err);
                    }
                }
            }

            copy(entry, &target_path)?;
            tracing::info!(?target_path, "Copied binary");
        }

        Settings::open()?.update_from_manifest(&self.manifest)?;

        Ok(())
    }

    /// Retrieves the sorted list of installed versions [`VersionManifest`]
    /// instances. In parallel, it also retrieves the active version if any.
    ///
    /// If theres any Active Version, the [`VersionManifest`] is not included
    /// as part of the first tuple value, instead it is returned as the second
    /// tuple value.
    pub fn scan_versions_manifests(
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

    /// Builds a "dummy" [`PackageSet`] from the current `manifest.json`.
    ///
    /// This is useful to perform operations that require a [`PackageSet`] instance,
    /// for example, version diffing.
    pub fn as_package_set(&self) -> Result<PackageSet> {
        if let Some(ref versioned_artifacts) = self.manifest.contents {
            let artifacts = versioned_artifacts
                .iter()
                .filter_map(|va| {
                    let Ok(version) = Version::parse(&va.version) else {
                        return None;
                    };

                    Some(Artifact {
                        version,
                        name: va.name.clone(),
                        download_url: String::from("N/A"),
                        sha256_url: String::from("N/A"),
                    })
                })
                .collect();
            let pkgset = PackageSet {
                pkgset: self.manifest.version.clone(),
                arch: String::from(TARGET),
                artifacts,
            };

            return Ok(pkgset);
        }

        tracing::debug!(version=%self.manifest.version, "The manifest containing artifacts details is not available");
        bail!(
            "No versioned artifacts manifest available for version: {}",
            self.manifest.version
        );
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{remove_file, remove_dir_all};
    use std::path::Path;

    use anyhow::Result;
    use fs_extra::dir::{copy as copy_dir, CopyOptions};
    use tempfile::TempDir;

    use fluvio_hub_util::sha256_digest;

    use crate::common::manifest::VersionedArtifact;
    use crate::common::settings::tests::{create_fvm_dir, delete_fvm_dir};

    use super::*;

    const TEST_BINARY_NAME: &str = "fluvio";
    const TEST_BINARY_CHECKSUM: &str =
        "31eba393bccae1973230a84b3d22b4c0dffb5d1ffa182e9e4dd6f1e4d7ba01af";

    /// Creates absolute path to version fixtures directory
    fn make_fixtures_path() -> PathBuf {
        let path = env!("CARGO_MANIFEST_DIR");
        let path = Path::new(path)
            .join("fixtures")
            .join("version")
            .join("0.10.14");

        path.to_path_buf()
    }

    /// Creates a Temporal Directory with the contents of the `fixtures/version`
    /// directory
    fn make_version_directory() -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let fixtures = make_fixtures_path();

        for entry in read_dir(fixtures)? {
            let entry = entry?;
            let entry_path = entry.path();
            let target_path = tmp.path().join(entry_path.file_name().unwrap());

            copy(&entry_path, &target_path)?;
        }

        Ok(tmp)
    }

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
    fn opens_directory_as_version_dir() {
        let tmpdir = make_version_directory().unwrap();
        let fixtures_path = make_fixtures_path();
        let version_dir = VersionDirectory::open(tmpdir.path().to_path_buf()).unwrap();

        assert_eq!(version_dir.contents.len(), 1);

        // Verify every binary in the Fixtures directory is in the Version
        // Directory
        let fixture_path = fixtures_path.join(TEST_BINARY_NAME);
        let tempdir_path = tmpdir.path().join(TEST_BINARY_NAME);

        assert!(fixture_path.exists(), "fixture binary should exist");
        assert!(tempdir_path.exists(), "tempdir binary should exist");
        assert!(
            version_dir.contents.contains(&tempdir_path),
            "version directory must include binary path as content"
        );
        assert_eq!(
            TEST_BINARY_CHECKSUM,
            sha256_digest(&tempdir_path).unwrap(),
            "binary contents must be the same"
        );

        // Compare manifest contents are the same
        let manifest =
            VersionManifest::open(fixtures_path.join(PACKAGE_SET_MANIFEST_FILENAME)).unwrap();

        assert_eq!(version_dir.manifest, manifest);
    }

    #[test]
    fn fails_to_open_directory_without_manifest() {
        let tmpdir = make_version_directory().unwrap();

        // Remove `manifest` on purpose
        let manifest_path = tmpdir.path().join(PACKAGE_SET_MANIFEST_FILENAME);
        remove_file(manifest_path).unwrap();

        // Attempts to open directory
        let result = VersionDirectory::open(tmpdir.path().to_path_buf());

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Failed to find manifest file in version directory"
        );
    }

    #[test]
    fn sets_as_active_directory() {
        create_fvm_dir();

        let tmpdir = make_version_directory().unwrap();
        let version_dir = VersionDirectory::open(tmpdir.path().to_path_buf()).unwrap();

        // Create fluvio directory
        let fluvio_bin_dir = fluvio_binaries_path().unwrap();

        if fluvio_bin_dir.exists() {
            remove_dir_all(&fluvio_bin_dir).unwrap();
        }

        create_dir_all(&fluvio_bin_dir).unwrap();

        let settings = Settings::open().unwrap();

        assert!(settings.version.is_none());
        assert!(settings.channel.is_none());

        version_dir.set_active().unwrap();

        let fluvio_path = fluvio_bin_dir.join(TEST_BINARY_NAME);
        let tempdir_path = tmpdir.path().join(TEST_BINARY_NAME);

        assert!(fluvio_path.exists(), "fluvio binary should exist");
        assert!(tempdir_path.exists(), "tempdir binary should exist");
        assert!(
            version_dir.contents.contains(&tempdir_path),
            "version directory must include binary path as content"
        );
        assert_eq!(
            TEST_BINARY_CHECKSUM,
            sha256_digest(&tempdir_path).unwrap(),
            "binary contents must be the same"
        );

        let settings = Settings::open().unwrap();

        assert_eq!(
            settings.version.unwrap(),
            version_dir.manifest.version.to_string()
        );
        assert_eq!(settings.channel.unwrap(), version_dir.manifest.channel);

        delete_fvm_dir();
    }

    #[test]
    fn lists_versions_in_dir() {
        let tmpdir = make_versions_directory().unwrap();
        let (manifests, _) = VersionDirectory::scan_versions_manifests(
            tmpdir.path().to_path_buf().join("version"),
            None,
        )
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
        let (_, active) = VersionDirectory::scan_versions_manifests(
            tmpdir.path().to_path_buf().join("version"),
            Some(Channel::Stable),
        )
        .unwrap();

        assert_eq!(active.unwrap().channel, Channel::Stable);
    }

    #[test]
    fn creates_pakageset_instance_from_version_dir() {
        let version_manifest = VersionManifest {
            channel: Channel::Stable,
            version: Version::parse("0.11.8").unwrap(),
            contents: Some(vec![
                VersionedArtifact {
                    name: String::from("fluvio"),
                    version: String::from("0.11.8"),
                },
                VersionedArtifact {
                    name: String::from("fluvio-cloud"),
                    version: String::from("0.2.22"),
                },
                VersionedArtifact {
                    name: String::from("cdk"),
                    version: String::from("0.11.8"),
                },
            ]),
        };
        let version_directory = VersionDirectory {
            manifest: version_manifest,
            path: PathBuf::new(),
            contents: vec![],
        };
        let package_set = PackageSet {
            pkgset: Version::parse("0.11.8").unwrap(),
            arch: TARGET.to_owned(),
            artifacts: vec![
                Artifact {
                    name: String::from("fluvio"),
                    version: Version::parse("0.11.8").unwrap(),
                    download_url: String::from("N/A"),
                    sha256_url: String::from("N/A"),
                },
                Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::parse("0.2.22").unwrap(),
                    download_url: String::from("N/A"),
                    sha256_url: String::from("N/A"),
                },
                Artifact {
                    name: String::from("cdk"),
                    version: Version::parse("0.11.8").unwrap(),
                    download_url: String::from("N/A"),
                    sha256_url: String::from("N/A"),
                },
            ],
        };

        assert_eq!(version_directory.as_package_set().unwrap(), package_set);
    }
}
