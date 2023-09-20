//! Fluvio Version Manager (FVM) type definitions.

use std::fmt::Display;
use std::str::FromStr;

use thiserror::Error;
use serde::{Deserialize, Serialize};
use url::Url;

pub const ARM_UNKNOWN_LINUX_GNUEABIHF: &str = "arm-unknown-linux-gnueabihf";
pub const ARMV7_UNKNOWN_LINUX_GNUEABIHF: &str = "armv7-unknown-linux-gnueabihf";
pub const X86_64_APPLE_DARWIN: &str = "x86_64-apple-darwin";
pub const AARCH64_APPLE_DARWIN: &str = "aarch64-apple-darwin";
pub const X86_64_PC_WINDOWS_GNU: &str = "x86_64-pc-windows-gnu";

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("The provided Rust Target Triple \"{0}\" is not supported")]
    RustTripleNotSupported(String),
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
    pub download_url: Url,
}

/// Fluvio Version Manager Package for a specific architecture and version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PackageSet {
    pub version: String,
    pub arch: RustTarget,
    pub artifacts: Vec<Artifact>,
}
