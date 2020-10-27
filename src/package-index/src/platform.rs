use std::fmt;
use serde::{Serialize, Deserialize};
use crate::Error;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Platform {
    #[serde(rename = "x86_64-apple-darwin")]
    X86_64AppleDarwin,
    #[serde(rename = "x86_64-unknown-linux-musl")]
    X86_64UnknownLinuxMusl,
}

impl Platform {
    pub fn as_str(&self) -> &str {
        match self {
            Self::X86_64AppleDarwin => "x86_64-apple-darwin",
            Self::X86_64UnknownLinuxMusl => "x86_64-unknown-linux-musl",
        }
    }
}

impl std::str::FromStr for Platform {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let platform = match s {
            "x86_64-apple-darwin" => Self::X86_64AppleDarwin,
            "x86_64-unknown-linux-musl" => Self::X86_64UnknownLinuxMusl,
            invalid => return Err(Error::InvalidPlatform(invalid.to_string())),
        };
        Ok(platform)
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
