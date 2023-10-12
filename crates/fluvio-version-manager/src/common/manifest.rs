//! Version Manifest
//!
//! The version manifest is a JSON file that contains the channel and version
//! for the binaries in a package set. It is used to determine the version of
//! the binaries

use std::fs::{read_to_string, write};
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use semver::Version;

use fluvio_hub_util::fvm::Channel;

/// The name of the manifest file for the Package Set
pub const PACKAGE_SET_MANIFEST_FILENAME: &str = "manifest.json";

#[derive(Debug, Deserialize, Serialize)]
pub struct VersionManifest {
    pub channel: Channel,
    pub version: Version,
}

impl VersionManifest {
    pub fn new(channel: Channel, version: Version) -> Self {
        Self { channel, version }
    }

    /// Opens the `manifest.json` file and parses it into a `VersionManifest` struct
    pub fn open(path: PathBuf) -> Result<Self> {
        let contents = read_to_string(path)?;

        Ok(serde_json::from_str(&contents)?)
    }

    /// Writes the JSON representation of the `VersionManifest` to the
    /// specified path
    pub fn write(&self, path: PathBuf) -> Result<PathBuf> {
        let json = serde_json::to_string_pretty(self)?;
        let path = path.join(PACKAGE_SET_MANIFEST_FILENAME);

        write(&path, json)?;
        Ok(path)
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_manifest_as_json() {
        let tempdir = TempDir::new().unwrap();
        let version_manifest = VersionManifest::new(Channel::Stable, Version::new(0, 8, 0));
        let json = serde_json::to_string_pretty(&version_manifest).unwrap();
        let manifest = version_manifest
            .write(tempdir.path().to_path_buf())
            .unwrap();

        assert_eq!(json, std::fs::read_to_string(manifest).unwrap());
    }

    #[test]
    fn reads_manifest_from_json() {
        let tempdir = TempDir::new().unwrap();
        let version_manifest = VersionManifest::new(Channel::Stable, Version::new(0, 8, 0));
        let json = serde_json::to_string_pretty(&version_manifest).unwrap();
        let manifest = version_manifest
            .write(tempdir.path().to_path_buf())
            .unwrap();
        let read_manifest = VersionManifest::open(manifest).unwrap();

        assert_eq!(json, serde_json::to_string_pretty(&read_manifest).unwrap());
    }

    #[test]
    fn fails_to_read_manifest_from_invalid_json() {
        let tempdir = TempDir::new().unwrap();
        let manifest = tempdir.path().join(PACKAGE_SET_MANIFEST_FILENAME);

        std::fs::write(manifest.clone(), "invalid json").unwrap();

        let result = VersionManifest::open(manifest);

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "expected value at line 1 column 1"
        );
    }
}
