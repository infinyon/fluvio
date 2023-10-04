//! Fluvio Version Manager (FVM) Types and HTTP Client.

mod api;

use std::fmt::Display;
use std::cmp::Ordering;
use std::str::FromStr;

use thiserror::Error;
use serde::{Deserialize, Serialize};
use semver::Version;
use url::Url;

pub use api::{Client, Download};

pub const STABLE_VERSION_CHANNEL: &str = "stable";
pub const LATEST_VERSION_CHANNEL: &str = "latest";
pub const DEFAULT_PKGSET: &str = "default";

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid Fluvio Channel \"{0}\"")]
    InvalidChannel(String),
}

/// Pacakge Set Channels based on Fluvio Channels
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Stable,
    Latest,
    Tag(Version),
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Channel::Stable => write!(f, "{}", STABLE_VERSION_CHANNEL),
            Channel::Latest => write!(f, "{}", LATEST_VERSION_CHANNEL),
            Channel::Tag(version) => write!(f, "{}", version),
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
            },
            Channel::Latest => match other {
                Channel::Stable => Ordering::Less,
                Channel::Latest => Ordering::Equal,
                Channel::Tag(_) => Ordering::Greater,
            },
            Channel::Tag(version) => match other {
                Channel::Stable => Ordering::Less,
                Channel::Latest => Ordering::Less,
                Channel::Tag(other_version) => version.cmp(other_version),
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
        matches!(self, Self::Tag(_))
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
                    Err(Error::InvalidChannel(s.to_string()))
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
    pub download_url: Url,
    pub sha256_url: Url,
}

/// Fluvio Version Manager Package for a specific architecture and version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PackageSet {
    pub version: Version,
    pub arch: String,
    pub artifacts: Vec<Artifact>,
}

#[cfg(test)]
mod tests {
    use super::Channel;

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
        let ver_b = Channel::parse("0.10.13").unwrap();

        assert!(ver_b > ver_a);
    }
}
