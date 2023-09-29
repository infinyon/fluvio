//! Fluvio Version Manager (FVM) Types and HTTP Client.

mod api;

use std::{fmt::Display, cmp::Ordering};
use std::str::FromStr;

use thiserror::Error;
use serde::{Deserialize, Serialize};
use semver::Version;
use url::Url;

pub use api::Client;

pub const STABLE_VERSION_CHANNEL: &str = "stable";
pub const LATEST_VERSION_CHANNEL: &str = "latest";

#[allow(unused)]
pub const DEFAULT_PKGSET: &str = "default";

pub const ARM_UNKNOWN_LINUX_GNUEABIHF: &str = "arm-unknown-linux-gnueabihf";
pub const ARMV7_UNKNOWN_LINUX_GNUEABIHF: &str = "armv7-unknown-linux-gnueabihf";
pub const X86_64_APPLE_DARWIN: &str = "x86_64-apple-darwin";
pub const AARCH64_APPLE_DARWIN: &str = "aarch64-apple-darwin";
pub const X86_64_PC_WINDOWS_GNU: &str = "x86_64-pc-windows-gnu";

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("The provided Rust Target Triple \"{0}\" is not supported")]
    RustTripleNotSupported(String),
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

/// Available Rust Targets for Fluvio.
///
/// Refer: https://github.com/infinyon/fluvio/blob/f2c49e126c771d58d24d5f5cb0282a6aaa6b23ca/.github/workflows/ci.yml#L141
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum RustTarget {
    /// arm-unknown-linux-gnueabihf
    ArmUnknownLinuxGnueabihf,
    /// armv7-unknown-linux-gnueabihf
    Armv7UnknownLinuxGnueabihf,
    /// x86_64-apple-darwin
    X86_64AppleDarwin,
    /// aarch64-apple-darwin
    Aarch64AppleDarwin,
    /// x86_64-pc-windows-gnu
    X86_64PcWindowsGnu,
}

impl Display for RustTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArmUnknownLinuxGnueabihf => write!(f, "{}", ARM_UNKNOWN_LINUX_GNUEABIHF),
            Self::Armv7UnknownLinuxGnueabihf => write!(f, "{}", ARMV7_UNKNOWN_LINUX_GNUEABIHF),
            Self::X86_64AppleDarwin => write!(f, "{}", X86_64_APPLE_DARWIN),
            Self::Aarch64AppleDarwin => write!(f, "{}", AARCH64_APPLE_DARWIN),
            Self::X86_64PcWindowsGnu => write!(f, "{}", X86_64_PC_WINDOWS_GNU),
        }
    }
}

impl FromStr for RustTarget {
    type Err = Error;

    fn from_str(v: &str) -> Result<Self, Self::Err> {
        match v {
            ARM_UNKNOWN_LINUX_GNUEABIHF => Ok(Self::ArmUnknownLinuxGnueabihf),
            ARMV7_UNKNOWN_LINUX_GNUEABIHF => Ok(Self::Armv7UnknownLinuxGnueabihf),
            X86_64_APPLE_DARWIN => Ok(Self::X86_64AppleDarwin),
            AARCH64_APPLE_DARWIN => Ok(Self::Aarch64AppleDarwin),
            X86_64_PC_WINDOWS_GNU => Ok(Self::X86_64PcWindowsGnu),
            _ => Err(Error::RustTripleNotSupported(v.to_string())),
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
    pub arch: RustTarget,
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
