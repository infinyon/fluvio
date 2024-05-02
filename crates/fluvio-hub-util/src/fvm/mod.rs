//! Fluvio Version Manager (FVM) Types and HTTP Client.

mod api;

use std::fmt::Display;
use std::cmp::Ordering;
use std::str::FromStr;
use std::collections::HashMap;

use thiserror::Error;
use serde::{Deserialize, Serialize};
use semver::Version;

pub use api::{Client, Download};

pub const STABLE_VERSION_CHANNEL: &str = "stable";
pub const LATEST_VERSION_CHANNEL: &str = "latest";
pub const DEFAULT_PKGSET: &str = "default";

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid Fluvio Channel \"{0}\"")]
    InvalidChannel(String),
}

/// Package Set Channels based on Fluvio Channels
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Stable,
    Latest,
    Tag(Version),
    Other(String),
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Channel::Stable => write!(f, "{}", STABLE_VERSION_CHANNEL),
            Channel::Latest => write!(f, "{}", LATEST_VERSION_CHANNEL),
            Channel::Tag(version) => write!(f, "{}", version),
            Channel::Other(version) => write!(f, "{}", version),
        }
    }
}

// Refer: https://rust-lang.github.io/rust-clippy/master/index.html#/derive_ord_xor_partial_ord
impl PartialOrd for Channel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Channel {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            Channel::Stable => match other {
                Channel::Stable => Ordering::Equal,
                Channel::Latest => Ordering::Greater,
                Channel::Tag(_) => Ordering::Greater,
                Channel::Other(_) => Ordering::Greater,
            },
            Channel::Latest => match other {
                Channel::Stable => Ordering::Less,
                Channel::Latest => Ordering::Equal,
                Channel::Tag(_) => Ordering::Greater,
                Channel::Other(_) => Ordering::Greater,
            },
            Channel::Tag(version) => match other {
                Channel::Stable => Ordering::Less,
                Channel::Latest => Ordering::Less,
                Channel::Tag(tag_version) => version.cmp(tag_version),
                Channel::Other(_) => Ordering::Less,
            },
            Channel::Other(version) => match other {
                Channel::Stable => Ordering::Less,
                Channel::Latest => Ordering::Less,
                Channel::Tag(_) => Ordering::Less,
                Channel::Other(other_version) => version.cmp(other_version),
            },
        }
    }
}

impl Channel {
    /// Parses the provided string into a [`Channel`].
    #[allow(unused)]
    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
        Self::from_str(s.as_ref())
    }

    /// Returns `true` if the instance is a version tag instead of a channel
    /// string.
    pub fn is_version_tag(&self) -> bool {
        matches!(self, Self::Tag(_) | Self::Other(_))
    }
}

impl FromStr for Channel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            STABLE_VERSION_CHANNEL => Ok(Self::Stable),
            LATEST_VERSION_CHANNEL => Ok(Self::Latest),
            _ => {
                if let Ok(version) = Version::parse(s) {
                    Ok(Self::Tag(version))
                } else {
                    Ok(Self::Other(s.to_string()))
                }
            }
        }
    }
}

/// Artifact download URL
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Artifact {
    pub name: String,
    pub version: Version,
    pub download_url: String,
    pub sha256_url: String,
}

/// Fluvio Version Manager Package for a specific architecture and version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PackageSetRecord {
    pub pkgset: String,
    pub arch: String,
    pub artifacts: Vec<Artifact>,
}

impl From<PackageSetRecord> for PackageSet {
    fn from(value: PackageSetRecord) -> Self {
        let fluvio_artifact = value.artifacts.iter().find(|art| art.name == "fluvio");
        let fluvio_version = fluvio_artifact
            .map(|art| art.version.clone())
            .unwrap_or_else(|| Version::new(0, 0, 0));

        PackageSet {
            pkgset: fluvio_version,
            arch: value.arch,
            artifacts: value.artifacts,
        }
    }
}

/// Fluvio Version Manager Package for a specific architecture and version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PackageSet {
    pub pkgset: Version,
    pub arch: String,
    pub artifacts: Vec<Artifact>,
}

impl PackageSet {
    /// Checks if artifacts in `b` are newer that artifacts in `self`.
    ///
    /// Patches included in [`PackageSet`]s are detected comparing versions
    /// at the `artifacts` level.
    ///
    /// Local [`PackageSet`] artifacts are compared with `upstream` to check
    /// for newer versions of these artifacts being available under the current
    /// Fluvio version.
    #[allow(dead_code)]
    fn check_artifact_updates(&self, upstream: &PackageSet) -> Vec<Artifact> {
        let ours: HashMap<String, Artifact> =
            self.artifacts.iter().fold(HashMap::new(), |mut map, art| {
                map.insert(art.name.to_owned(), art.to_owned());
                map
            });
        let theirs: HashMap<String, Artifact> =
            upstream
                .artifacts
                .iter()
                .fold(HashMap::new(), |mut map, art| {
                    map.insert(art.name.to_owned(), art.to_owned());
                    map
                });
        let mut new_artifacts: Vec<Artifact> = Vec::with_capacity(theirs.len());

        for (art_name, our_artifact) in ours {
            let Some(their_artifact) = theirs.get(&art_name) else {
                tracing::warn!(%art_name, "Found missing artifact in incoming PackageSet");
                continue;
            };

            if their_artifact.version > our_artifact.version {
                new_artifacts.push(their_artifact.to_owned());
            }
        }

        new_artifacts
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Artifact, Channel, PackageSet, Version};

    #[test]
    fn parses_latest_channel_from_str() {
        let channel = Channel::parse("latest").unwrap();

        assert_eq!(channel, Channel::Latest);
    }

    #[test]
    fn parses_stable_channel_from_str() {
        let channel = Channel::parse("stable").unwrap();

        assert_eq!(channel, Channel::Stable);
    }

    #[test]
    fn determines_stable_as_greater_than_latest() {
        let stable = Channel::parse("stable").unwrap();
        let latest = Channel::parse("latest").unwrap();

        assert!(stable > latest);
    }

    #[test]
    fn performs_comparisons_between_tags() {
        let ver_a = Channel::parse("0.10.10").unwrap();
        let ver_b = Channel::parse("0.10.13-mirroring347239873+20231016").unwrap();

        assert!(ver_b > ver_a);
    }

    #[test]
    fn determines_otherversion_revisioning() {
        let stable = Channel::parse("stable").unwrap();
        let ssdkp1 = Channel::parse("ssdk-preview1").unwrap();
        let ssdkp2 = Channel::parse("ssdk-preview2").unwrap();

        assert!(stable > ssdkp1);
        assert!(ssdkp2 > ssdkp1);
    }

    #[test]
    fn determines_if_other_packageset_includes_newer_artifacts() {
        let package_sets = vec![
        (
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![
                Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::from_str("0.2.19").unwrap(),
                    download_url: String::from("https://packages.fluvio.io/fluvio-cloud/aarch64-apple-darwin/0.2.19"),
                    sha256_url: String::from("https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/0.2.19/aarch64-apple-darwin/fluvio-cloud.sha256"),
                }
                ]
            },
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![
                Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::from_str("0.11.6").unwrap(),
                    download_url: String::from("https://packages.fluvio.io/fluvio-cloud/aarch64-apple-darwin/0.2.19"),
                    sha256_url: String::from("https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/0.2.19/aarch64-apple-darwin/fluvio-cloud.sha256"),
                }
                ]
            },
            1
        ),
        (
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![
                Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::from_str("0.2.19").unwrap(),
                    download_url: String::from("https://packages.fluvio.io/fluvio-cloud/aarch64-apple-darwin/0.2.19"),
                    sha256_url: String::from("https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/0.2.19/aarch64-apple-darwin/fluvio-cloud.sha256"),
                }
                ]
            },
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![
 Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::from_str("0.2.19").unwrap(),
                    download_url: String::from("https://packages.fluvio.io/fluvio-cloud/aarch64-apple-darwin/0.2.19"),
                    sha256_url: String::from("https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/0.2.19/aarch64-apple-darwin/fluvio-cloud.sha256"),
                }
                ]
            },
            0
        ),
                (
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![
                Artifact {
                    name: String::from("fluvio-cloud"),
                    version: Version::from_str("0.2.19").unwrap(),
                    download_url: String::from("https://packages.fluvio.io/fluvio-cloud/aarch64-apple-darwin/0.2.19"),
                    sha256_url: String::from("https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/0.2.19/aarch64-apple-darwin/fluvio-cloud.sha256"),
                }
                ]
            },
            PackageSet {
                pkgset: Version::from_str("0.11.7").unwrap(),
                arch: String::from("aarch64-apple-darwin"),
                artifacts: vec![]
            },
            0
        )
        ];

        for (ours, theirs, new_pkgs) in package_sets {
            assert_eq!(ours.check_artifact_updates(&theirs).len(), new_pkgs);
        }
    }
}
