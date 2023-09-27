use std::fs::read_to_string;
use std::path::Path;

use semver::Version;
use serde::{Serialize, Deserialize};

use fluvio_hub_util::fvm::{PackageSet, RustTarget};

use crate::Result;

use super::PackageError;

pub const MANIFEST_FILENAME: &str = "manifest.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactMainfest {
    pub name: String,
    pub version: Version,
}

/// Package Manifest used to store the Package Set Metadata, along with included
/// binaries list from installation and the specific version of the Package Set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackageManifest {
    pub arch: RustTarget,
    pub artifacts: Vec<ArtifactMainfest>,
    pub version: Version,
}

impl From<PackageSet> for PackageManifest {
    fn from(pkgset: PackageSet) -> Self {
        Self {
            arch: pkgset.arch,
            artifacts: pkgset
                .artifacts
                .into_iter()
                .map(|a| ArtifactMainfest {
                    name: a.name,
                    version: a.version,
                })
                .collect(),
            version: pkgset.version,
        }
    }
}

impl PackageManifest {
    /// Opens the Package Manifest from the specified path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let manifest = read_to_string(path)?;
        let manifest = serde_json::from_str(&manifest).map_err(PackageError::ManifestParse)?;

        Ok(manifest)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self).map_err(PackageError::ManifestSerialization)?)
    }
}
