//!
//! # SmartModule Package
//!

use std::{
    io::Error as IoError,
    fmt::{Display, Formatter},
};

use bytes::Buf;
use semver::Version as SemVersion;
use thiserror::Error;

use fluvio_protocol::{Encoder, Decoder, Version};

use super::params::SmartModuleParams;

#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleMetadata {
    pub package: SmartModulePackage,
    pub params: SmartModuleParams,
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

    #[cfg(feature = "smartmodule")]
    /// parse the metadata bytes and return the metadata
    pub fn from_bytes(bytedata: &[u8]) -> std::io::Result<Self> {
        use std::io::ErrorKind;
        let strdata = std::str::from_utf8(bytedata)
            .map_err(|_| IoError::new(ErrorKind::InvalidData, "Smartmodule toml"))?;
        let metadata = toml::from_str(strdata)?;
        Ok(metadata)
    }

    /// id that can be used to identify this smartmodule
    pub fn store_id(&self) -> String {
        self.package.store_id()
    }
}

/// SmartModule package definition
/// This is defined in the `SmartModule.toml` in the root of the SmartModule project
#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartModulePackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub api_version: FluvioSemVersion,
    pub description: Option<String>,
    pub license: Option<String>,

    #[fluvio(min_version = 19)]
    #[cfg_attr(
        feature = "use_serde",
        serde(default = "SmartModulePackage::visibility_if_missing")
    )]
    pub visibility: SmartModuleVisibility,
    pub repository: Option<String>,
}

impl SmartModulePackage {
    /// id that can be used to identify this smartmodule
    pub fn store_id(&self) -> String {
        (SmartModulePackageKey {
            name: self.name.clone(),
            group: Some(self.group.clone()),
            version: Some(self.version.clone()),
        })
        .store_id()
    }

    pub fn is_valid(&self) -> bool {
        !self.name.is_empty() && !self.group.is_empty()
    }

    pub fn fqdn(&self) -> String {
        format!(
            "{}{}{}{}{}",
            self.group, GROUP_SEPARATOR, self.name, VERSION_SEPARATOR, self.version
        )
    }

    pub fn visibility_if_missing() -> SmartModuleVisibility {
        SmartModuleVisibility::Private
    }
}

#[derive(Debug, Error)]
pub enum SmartModuleKeyError {
    #[error("SmartModule version`{version}` is not valid because {error}")]
    InvalidVersion { version: String, error: String },
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum SmartModuleVisibility {
    #[default]
    Private,
    Public,
}

#[derive(Default)]
pub struct SmartModulePackageKey {
    pub name: String,
    pub group: Option<String>,
    pub version: Option<FluvioSemVersion>,
}

const GROUP_SEPARATOR: char = '/';
const VERSION_SEPARATOR: char = '@';

impl SmartModulePackageKey {
    /// convert from qualified name into package info
    /// qualified name is in format of "group/name@version"
    pub fn from_qualified_name(fqdn: &str) -> Result<Self, SmartModuleKeyError> {
        let mut pkg = Self::default();
        let mut split = fqdn.split(GROUP_SEPARATOR);
        let first_token = split.next().unwrap().to_owned();
        if let Some(name_part) = split.next() {
            // group name is found
            pkg.group = Some(first_token);
            // let split name part
            let mut version_split = name_part.split(VERSION_SEPARATOR);
            let second_token = version_split.next().unwrap().to_owned();
            if let Some(version_part) = version_split.next() {
                // version is found
                pkg.name = second_token;
                pkg.version = Some(FluvioSemVersion::new(
                    lenient_semver::parse(version_part).map_err(|err| {
                        SmartModuleKeyError::InvalidVersion {
                            version: version_part.to_owned(),
                            error: err.to_string(),
                        }
                    })?,
                ));
                Ok(pkg)
            } else {
                // no version found
                pkg.name = second_token;
                Ok(pkg)
            }
        } else {
            // no group parameter is specified, in this case we treat as name
            pkg.name = first_token;
            Ok(pkg)
        }
    }

    /// Check if key matches against name and package
    /// if package doesn't exists then it should match name only
    /// otherwise it should match against package
    pub fn is_match(&self, name: &str, package: Option<&SmartModulePackage>) -> bool {
        if let Some(package) = package {
            if let Some(version) = &self.version {
                if package.version != *version {
                    return false;
                }
            }

            if let Some(group) = &self.group {
                if package.group != *group {
                    return false;
                }
            }

            self.name == package.name
        } else {
            self.name == name
        }
    }

    /// return key for storing SmartModule in the store
    pub fn store_id(&self) -> String {
        let group_id = if let Some(package) = &self.group {
            format!("-{}", package)
        } else {
            "".to_owned()
        };

        let version_id = if let Some(version) = &self.version {
            format!("-{}", version)
        } else {
            "".to_owned()
        };

        format!("{}{}{}", self.name, group_id, version_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FluvioSemVersion(SemVersion);

impl FluvioSemVersion {
    pub fn parse(version: &str) -> Result<Self, semver::Error> {
        Ok(Self(SemVersion::parse(version)?))
    }

    pub fn new(version: SemVersion) -> Self {
        Self(version)
    }
}

impl Default for FluvioSemVersion {
    fn default() -> Self {
        Self(SemVersion::new(0, 1, 0))
    }
}

impl Display for FluvioSemVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Encoder for FluvioSemVersion {
    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
        self.0.to_string().write_size(version)
    }

    fn encode<T>(
        &self,
        dest: &mut T,
        version: fluvio_protocol::Version,
    ) -> Result<(), std::io::Error>
    where
        T: bytes::BufMut,
    {
        self.0.to_string().encode(dest, version)
    }
}

impl Decoder for FluvioSemVersion {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        let mut version_str = String::from("");
        version_str.decode(src, version)?;
        let version = SemVersion::parse(&version_str)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        self.0 = version;
        Ok(())
    }
}

impl std::fmt::Display for SmartModuleVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lbl = match self {
            Self::Private => "private",
            Self::Public => "public",
        };
        write!(f, "{}", lbl)
    }
}

impl std::convert::TryFrom<&str> for SmartModuleVisibility {
    type Error = &'static str;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "private" => Ok(SmartModuleVisibility::Private),
            "public" => Ok(SmartModuleVisibility::Public),
            _ => Err("Only private or public is allowed"),
        }
    }
}

/// Convert from name into something that can be used as key in the store
/// For now, we respect

#[cfg(test)]
mod package_test {
    use crate::smartmodule::SmartModulePackageKey;

    use super::{SmartModulePackage, FluvioSemVersion};

    #[test]
    fn test_pkg_validation() {
        assert!(SmartModulePackage {
            name: "a".to_owned(),
            group: "b".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        }
        .is_valid());

        assert!(!SmartModulePackage {
            name: "".to_owned(),
            group: "b".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        }
        .is_valid());

        assert!(!SmartModulePackage {
            name: "c".to_owned(),
            group: "".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        }
        .is_valid());

        assert!(!SmartModulePackage {
            name: "".to_owned(),
            group: "".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        }
        .is_valid());
    }

    #[test]
    fn test_pkg_fqdn() {
        let pkg = SmartModulePackage {
            name: "test".to_owned(),
            group: "fluvio".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };

        assert_eq!(pkg.fqdn(), "fluvio/test@0.1.0");
    }

    #[test]
    fn test_pkg_name() {
        let pkg = SmartModulePackage {
            name: "test".to_owned(),
            group: "fluvio".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };

        assert_eq!(pkg.store_id(), "test-fluvio-0.1.0");
    }

    #[test]
    fn test_pkg_key_fully_qualified() {
        let pkg =
            SmartModulePackageKey::from_qualified_name("mygroup/module1@0.1.0").expect("parse");
        assert_eq!(pkg.name, "module1");
        assert_eq!(pkg.group, Some("mygroup".to_owned()));
        assert_eq!(
            pkg.version,
            Some(FluvioSemVersion::parse("0.1.0").expect("parse"))
        );
    }

    #[test]
    fn test_pkg_key_name_only() {
        let pkg = SmartModulePackageKey::from_qualified_name("module2").expect("parse");
        assert_eq!(pkg.name, "module2");
        assert_eq!(pkg.group, None);
        assert_eq!(pkg.version, None);
    }

    #[test]
    fn test_pkg_key_group() {
        let pkg = SmartModulePackageKey::from_qualified_name("group1/module2").expect("parse");
        assert_eq!(pkg.name, "module2");
        assert_eq!(pkg.group, Some("group1".to_owned()));
        assert_eq!(pkg.version, None);
    }

    #[test]
    fn test_pkg_key_versions() {
        assert!(SmartModulePackageKey::from_qualified_name("group1/module2@10.").is_err());
        assert!(SmartModulePackageKey::from_qualified_name("group1/module2@").is_err());
        assert!(SmartModulePackageKey::from_qualified_name("group1/module2@10").is_ok());
        assert!(SmartModulePackageKey::from_qualified_name("group1/module2@10.2").is_ok());
    }

    #[test]
    fn test_pkg_key_match() {
        let key =
            SmartModulePackageKey::from_qualified_name("mygroup/module1@0.1.0").expect("parse");
        let valid_pkg = SmartModulePackage {
            name: "module1".to_owned(),
            group: "mygroup".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };
        assert!(key.is_match(&valid_pkg.store_id(), Some(&valid_pkg)));
        assert!(
            SmartModulePackageKey::from_qualified_name("mygroup/module1")
                .expect("parse")
                .is_match(&valid_pkg.store_id(), Some(&valid_pkg))
        );
        assert!(SmartModulePackageKey::from_qualified_name("module1")
            .expect("parse")
            .is_match(&valid_pkg.store_id(), Some(&valid_pkg)));
        assert!(!SmartModulePackageKey::from_qualified_name("module2")
            .expect("parse")
            .is_match(&valid_pkg.store_id(), Some(&valid_pkg)));

        let in_valid_pkg = SmartModulePackage {
            name: "module2".to_owned(),
            group: "mygroup".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };
        assert!(!key.is_match(&in_valid_pkg.store_id(), Some(&in_valid_pkg)));

        assert!(SmartModulePackageKey::from_qualified_name("module1")
            .expect("parse")
            .is_match("module1", None));
    }

    #[test]
    fn test_pk_key_store_id() {
        assert_eq!(
            SmartModulePackageKey::from_qualified_name("module1")
                .expect("parse")
                .store_id(),
            "module1"
        );
        assert_eq!(
            SmartModulePackageKey::from_qualified_name("mygroup/module1@0.1")
                .expect("parse")
                .store_id(),
            "module1-mygroup-0.1.0"
        );
    }
}

#[cfg(all(test, feature = "smartmodule"))]
mod test {

    use crate::smartmodule::params::{SmartModuleParams, SmartModuleParam};

    use super::{FluvioSemVersion, SmartModulePackage};

    #[test]
    fn write_metadata_toml() {
        let pkg = SmartModulePackage {
            name: "test".to_owned(),
            group: "group".to_owned(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };

        let param = SmartModuleParam {
            optional: true,
            description: Some("fluvio".to_owned()),
        };
        let mut params = SmartModuleParams::default();
        params.insert_param("param1".to_owned(), param);
        let metadata = super::SmartModuleMetadata {
            package: pkg,
            params,
        };

        let toml = toml::to_string(&metadata).expect("toml");
        println!("{}", toml);
        assert!(toml.contains("param1"));
    }

    #[test]
    fn read_metadata_toml() {
        let metadata = super::SmartModuleMetadata::from_toml("tests/SmartModule.toml")
            .expect("failed to parse metadata");
        assert_eq!(metadata.package.name, "MyCustomModule");
        assert_eq!(
            metadata.package.version,
            FluvioSemVersion::parse("0.1.0").unwrap()
        );
        assert_eq!(metadata.package.description.unwrap(), "My Custom module");
        assert_eq!(
            metadata.package.api_version,
            FluvioSemVersion::parse("0.1.0").unwrap()
        );
        assert_eq!(metadata.package.license.unwrap(), "Apache-2.0");
        assert_eq!(
            metadata.package.repository.unwrap(),
            "https://github.com/infinyon/fluvio"
        );

        let params = metadata.params;
        assert_eq!(params.len(), 2);
        let input1 = &params.get_param("multiplier").unwrap();
        assert_eq!(input1.description.as_ref().unwrap(), "multiply input");
        assert!(!input1.optional);
    }
}
