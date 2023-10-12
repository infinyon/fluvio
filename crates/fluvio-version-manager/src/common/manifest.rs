//! Version Manifest
//!
//! The version manifest is a JSON file that contains the channel and version
//! for the binaries in a package set. It is used to determine the version of
//! the binaries

use std::fs::write;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use semver::Version;

use fluvio_hub_util::fvm::Channel;

/// The name of the manifest file for the Package Set
const PACKAGE_SET_MANIFEST_FILENAME: &str = "manifest.json";

#[derive(Debug, Deserialize, Serialize)]
pub struct VersionManifest {
    pub channel: Channel,
    pub version: Version,
}

impl VersionManifest {
    pub fn new(channel: Channel, version: Version) -> Self {
        Self { channel, version }
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
}
