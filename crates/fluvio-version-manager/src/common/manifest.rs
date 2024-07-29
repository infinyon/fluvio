//! Version Manifest
//!
//! The version manifest is a JSON file that contains the channel and version
//! for the binaries in a package set. It is used to determine the version of
//! the binaries

use std::fs::{read_to_string, write};
use std::path::{PathBuf, Path};
use std::str::FromStr;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use semver::Version;

use fluvio_hub_util::fvm::Channel;

/// The name of the manifest file for the Package Set
pub const PACKAGE_SET_MANIFEST_FILENAME: &str = "manifest.json";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VersionedArtifact {
    pub name: String,
    pub version: String,
}

impl VersionedArtifact {
    pub fn new<S: Into<String>>(name: S, version: S) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VersionManifest {
    pub channel: Channel,
    pub version: Version,
    pub contents: Option<Vec<VersionedArtifact>>,
}

impl VersionManifest {
    pub fn new(channel: Channel, version: Version, contents: Vec<VersionedArtifact>) -> Self {
        Self {
            channel,
            version,
            contents: Some(contents),
        }
    }

    /// Opens the `manifest.json` file and parses it into a `VersionManifest` struct
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let contents = read_to_string(path)?;

        Ok(serde_json::from_str(&contents)?)
    }

    /// Writes the JSON representation of the `VersionManifest` to the
    /// specified path
    pub fn write(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let json = serde_json::to_string_pretty(self)?;
        let path = path.as_ref().join(PACKAGE_SET_MANIFEST_FILENAME);

        write(&path, json)?;
        Ok(path)
    }
}

impl FromStr for VersionManifest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_json::from_str(s)?)
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_manifest_as_json() {
        const WANT: &str = r#"{
  "channel": "stable",
  "version": "0.8.0",
  "contents": [
    {
      "name": "fluvio",
      "version": "0.11.4"
    }
  ]
}"#;
        let tempdir = TempDir::new().unwrap();
        let version_manifest = VersionManifest::new(
            Channel::Stable,
            Version::new(0, 8, 0),
            vec![VersionedArtifact::new("fluvio", "0.11.4")],
        );
        let manifest = version_manifest.write(tempdir.path()).unwrap();
        let have = read_to_string(manifest).unwrap();

        assert_eq!(have, WANT);
    }

    #[test]
    fn reads_manifest_from_json() {
        let tempdir = TempDir::new().unwrap();
        let version_manifest = VersionManifest::new(
            Channel::Stable,
            Version::new(0, 8, 0),
            vec![VersionedArtifact::new("fluvio", "0.11.4")],
        );
        let json = serde_json::to_string_pretty(&version_manifest).unwrap();
        let manifest = version_manifest.write(tempdir.path()).unwrap();
        let read_manifest = VersionManifest::open(manifest).unwrap();

        assert_eq!(json, serde_json::to_string_pretty(&read_manifest).unwrap());
    }

    #[test]
    fn fails_to_read_manifest_from_invalid_json() {
        const INVALID_MANIFEST: &str = r#"{
"foo": "bar",
"hello": "world"
}"#;

        let result = VersionManifest::from_str(INVALID_MANIFEST);

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "missing field `channel` at line 4 column 1"
        );
    }
}
