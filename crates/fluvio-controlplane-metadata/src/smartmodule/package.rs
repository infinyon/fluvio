//!
//! # SmartModule Package
//!

use std::io::Error as IoError;

use bytes::Buf;
use fluvio_protocol::{Encoder, Decoder, Version};
use semver::Version as SemVersion;


/// SmartModule package definition
/// This is defined in the `SmartModule.toml` in the root of the SmartModule project
#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModulePackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub edition: FluvioSemVersion,
    pub description: String,
    #[serde(default)]
    pub authors: Vec<String>,
    pub license: String,
    pub repository: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FluvioSemVersion(SemVersion);

impl Default for FluvioSemVersion {
    fn default() -> Self {
        Self(SemVersion::new(0, 1, 0))
    }
}

impl Encoder for FluvioSemVersion {
    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
        self.0.to_string().write_size(version)
    }

    fn encode<T>(&self, dest: &mut T, version: fluvio_protocol::Version) -> Result<(), std::io::Error>
    where
        T: bytes::BufMut {
            
            self.0.to_string().encode(dest,version)
    }
}

impl Decoder for FluvioSemVersion {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf {
            let mut version_str = String::from("");
            version_str.decode(src,version)?;
            let version = SemVersion::parse(&version_str).map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
            self.0 = version;
            Ok(())
    }
}




