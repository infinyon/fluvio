use std::fmt;
use serde::{Serialize, Deserialize};
use crate::Error;

const PACKAGE_TARGET: &str = env!("PACKAGE_TARGET");

/// Detects the target triple of the current build and returns
/// the name of a compatible build target on packages.fluvio.io.
///
/// Returns `Some(Target)` if there is a compatible target, or
/// `None` if this target is unsupported or has no compatible target.
pub fn package_target() -> Result<Target, Error> {
    let target = PACKAGE_TARGET.parse()?;
    Ok(target)
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Target {
    #[serde(rename = "x86_64-apple-darwin")]
    X86_64AppleDarwin,
    #[serde(rename = "x86_64-unknown-linux-musl")]
    X86_64UnknownLinuxMusl,
}

impl Target {
    pub fn as_str(&self) -> &str {
        match self {
            Self::X86_64AppleDarwin => "x86_64-apple-darwin",
            Self::X86_64UnknownLinuxMusl => "x86_64-unknown-linux-musl",
        }
    }
}

impl std::str::FromStr for Target {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let platform = match s {
            "x86_64-apple-darwin" => Self::X86_64AppleDarwin,
            "x86_64-unknown-linux-musl" => Self::X86_64UnknownLinuxMusl,
            "x86_64-unknown-linux-gnu" => Self::X86_64UnknownLinuxMusl,
            invalid => return Err(Error::InvalidPlatform(invalid.to_string())),
        };
        Ok(platform)
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
