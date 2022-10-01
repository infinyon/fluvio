//!
//! # SmartModule Package
//!

use std::{io::Error as IoError};

use bytes::Buf;
use semver::Version as SemVersion;

use fluvio_protocol::{Encoder, Decoder, Version};


#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleMetadata {
    pub package: SmartModulePackage,
    pub params: Vec<SmartModuleParam>,
}

impl SmartModuleMetadata {
    #[cfg(feature = "smartmodule")]
    /// parse the metadata file and return the metadata
    pub fn from_toml<T: AsRef<std::path::Path>>(path: T) -> std::io::Result<Self> {
        use std::fs::read_to_string;

        let path_ref = path.as_ref();
        let file_str: String = read_to_string(path_ref)?;
        let metadata = toml::from_str(&file_str)?;
        Ok(metadata)
    }
}


/// SmartModule package definition
/// This is defined in the `SmartModule.toml` in the root of the SmartModule project
#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModulePackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub apiVersion: FluvioSemVersion,
    pub description: Option<String>,
    pub license: Option<String>,
    pub repository: Option<String>,
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



#[derive(Debug, Clone, PartialEq, Eq, Encoder, Default, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleParam {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
}


    

#[cfg(test)]
mod test {

    use semver::Version;

    #[test]
    fn read_metadata_toml() {
        let metadata = super::SmartModuleMetadata::from_toml("tests/regex.toml")
            .expect("failed to parse metadata");
        assert_eq!(metadata.package.name, "regex");
        assert_eq!(metadata.package.version, Version::parse("0.1.0").unwrap());
        assert_eq!(metadata.package.description, "Regex SmartModule");
        assert_eq!(metadata.package.authors.len(), 1);
        assert_eq!(metadata.package.authors[0], "Infinyon");
        assert_eq!(metadata.package.package_version, "0.1");
        assert_eq!(metadata.package.license, "Apache-2.0");
        assert_eq!(
            metadata.package.repository,
            "https://github.com/infinyon/fluvio"
        );
        let init_param = metadata.init;
        assert_eq!(init_param.len(), 1);
        assert_eq!(init_param["regex"].input, super::InitType::String);
    }
}

