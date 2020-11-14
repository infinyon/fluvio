use std::fmt;
use serde::{Serialize, Deserialize, Deserializer};
use url::Url;
use crate::Error;
use semver::Version;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Registry(url::Url);

impl Registry {
    fn try_from_segments(segments: &[&str]) -> Option<Self> {
        if segments.is_empty() {
            return None;
        }

        let mut reconstructed: String = segments.join("/");
        if !reconstructed.ends_with('/') {
            reconstructed.push('/');
        }
        let registry_url = url::Url::parse(&reconstructed).ok()?;
        let registry = Registry::from(registry_url);
        Some(registry)
    }
}

lazy_static::lazy_static! {
    static ref DEFAULT_REGISTRY: Registry = {
        let url = url::Url::parse("https://packages.fluvio.io/v1/").unwrap();
        Registry::from(url)
    };
}

impl fmt::Display for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Default for Registry {
    fn default() -> Self {
        DEFAULT_REGISTRY.clone()
    }
}

impl std::str::FromStr for Registry {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(s).map_err(Error::FailedToParseRegistry)?;
        Ok(Self(url))
    }
}

impl From<url::Url> for Registry {
    fn from(url: url::Url) -> Self {
        Self(url)
    }
}

impl AsRef<url::Url> for Registry {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

/// Certain newtypes are just Strings but cannot contain a '/' character.
///
/// This macro is used to derive the deserialization rules for those types.
macro_rules! deserialize_no_slash_string {
    ($mod:ident, $id:ident, $err:expr, $string_name:expr $(,)?) => {
        mod $mod {
            use super::$id;
            use super::Error;
            impl std::str::FromStr for $id {
                type Err = Error;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    if s.find('/').is_some() {
                        return Err($err("cannot contain '/'".to_string()));
                    }
                    if s.find(':').is_some() {
                        return Err($err("cannot contain ':'".to_string()));
                    }
                    return Ok(Self(s.to_string()));
                }
            }

            impl $id {
                pub fn as_str(&self) -> &str {
                    &self.0
                }
            }

            impl<'de> serde::Deserialize<'de> for $id {
                fn deserialize<D>(
                    deserializer: D,
                ) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    let string = String::deserialize(deserializer)?;

                    // Reject strings that have `/` in them
                    if let Err(e) = string.parse::<$id>() {
                        use serde::de::{Unexpected, Error as DeserializeError};
                        return Err(DeserializeError::invalid_value(
                            Unexpected::Other(&e.to_string()),
                            &&*format!("valid {}", $string_name),
                        ));
                    }

                    Ok(Self(string))
                }
            }
        }
    };
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct GroupName(String);

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

deserialize_no_slash_string!(group, GroupName, Error::InvalidGroupName, "index group");

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PackageName(String);

impl fmt::Display for PackageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

deserialize_no_slash_string!(
    package_name,
    PackageName,
    Error::InvalidPackageName,
    "package name"
);

pub type WithVersion = Version;
pub type MaybeVersion = Option<Version>;

/// A unique identifier for a package that describes its registry, group,
/// name, and (possibly) version.
///
/// There are two different variations of a PackageId, which have different
/// formatting and parsing rules.
///
/// 1) A `PackageId<WithVersion>` represents a fully-qualified package
/// name that also refers to a specific version of the package. It is
/// rendered (and parsed) as a string in the following form:
///
/// ```text
/// <registry>/<group>/<name>:<version>
/// OR
/// <group>/<name>:<version>
/// ```
///
/// Note that a `PackageId<WithVersion>` has strong guarantees on its
/// `FromStr` implementation (and therefore, it's `parse()` rules). This
/// means that a `PackageId<WithVersion>` is _guaranteed_ to have a version
/// embedded in it, which can be accessed via `.version()`.
///
/// 2) A `PackageId<MaybeVersion>` might or might not contain a version, and
/// will parse a package string that does OR does not have a version in it.
/// This is the type you should use if you don't need a version or if you want
/// to do something different based on whether or not a version is given. An
/// example of this might be installing a specific version of a package if a
/// version is given, or defaulting to the latest version if not given.
///
/// Valid strings that will parse into a `PackageId<MaybeVersion>` include:
///
/// ```text
/// <registry>/<group>/<name>
/// <group>/<name>
/// <registry>/<group>/<name>:<version>
/// <group>/<name>:<version>
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct PackageId<V> {
    registry: Option<Registry>,
    pub group: GroupName,
    pub name: PackageName,
    version: V,
}

impl<T> PackageId<T> {
    pub fn registry(&self) -> &Registry {
        match self.registry.as_ref() {
            Some(registry) => registry,
            None => &*DEFAULT_REGISTRY,
        }
    }

    pub fn display(&self) -> impl fmt::Display {
        let registry = self.registry.as_ref().map(|it| it.0.as_str()).unwrap_or("");
        format!(
            "{registry}{group}/{name}",
            registry = registry,
            group = self.group.as_str(),
            name = self.name.as_str(),
        )
    }
}

impl PackageId<WithVersion> {
    /// Create a new, versioned PackageId.
    pub fn new_versioned(name: PackageName, group: GroupName, version: Version) -> Self {
        Self {
            registry: None,
            group,
            name,
            version,
        }
    }

    /// A PackageId<WithVersion> indisputably has a version, no Option required.
    pub fn version(&self) -> &Version {
        &self.version
    }
}

impl PackageId<MaybeVersion> {
    /// Create a new PackageId with no version info.
    pub fn new_unversioned(name: PackageName, group: GroupName) -> Self {
        PackageId {
            registry: None,
            group,
            name,
            version: None,
        }
    }

    /// Add a Version to any PackageId to create a PackageId<WithVersion>
    pub fn into_versioned(self, version: Version) -> PackageId<WithVersion> {
        PackageId {
            registry: self.registry,
            name: self.name,
            group: self.group,
            version,
        }
    }
}

impl PackageId<MaybeVersion> {
    pub fn maybe_version(&self) -> Option<&Version> {
        self.version.as_ref()
    }
}

impl std::str::FromStr for PackageId<WithVersion> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut segments: Vec<&str> = s.split('/').collect();
        if segments.len() < 2 {
            return Err(Error::TooFewSlashes);
        }

        let name_version_segment = segments.pop().unwrap();
        let name_version_segments: Vec<&str> = name_version_segment.split(':').collect();
        let (name_string, version_string) = match &name_version_segments[..] {
            [name_string, version_string] => (name_string, version_string),
            _ => return Err(Error::InvalidNameVersionSegment),
        };

        let version = Version::parse(version_string)?;
        let name: PackageName = name_string.parse()?;

        let group_string = segments.pop().unwrap();
        let group: GroupName = group_string.parse()?;
        let registry = Registry::try_from_segments(&segments);

        let package_id = PackageId {
            registry,
            group,
            name,
            version,
        };

        Ok(package_id)
    }
}

impl std::str::FromStr for PackageId<MaybeVersion> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut segments: Vec<&str> = s.split('/').collect();
        if segments.len() < 2 {
            return Err(Error::TooFewSlashes);
        }

        let name_version_segment = segments.pop().unwrap();
        let name_version_segments: Vec<&str> = name_version_segment.split(':').collect();
        let (name_string, version_string) = match &name_version_segments[..] {
            [name_string] => (name_string, None),
            [name_string, version_string] => (name_string, Some(version_string)),
            _ => return Err(Error::InvalidNameVersionSegment),
        };

        let version = match version_string {
            Some(version_string) => Some(Version::parse(version_string)?),
            None => None,
        };
        let name: PackageName = name_string.parse()?;
        let group_string = segments.pop().unwrap();
        let group: GroupName = group_string.parse()?;
        let registry = Registry::try_from_segments(&segments);

        let package_id = PackageId {
            registry,
            group,
            name,
            version,
        };

        Ok(package_id)
    }
}

impl fmt::Display for PackageId<WithVersion> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = self.registry.as_ref().map(|it| it.0.as_str()).unwrap_or("");
        write!(
            f,
            "{registry}{group}/{name}:{version}",
            registry = registry,
            group = self.group.as_str(),
            name = self.name.as_str(),
            version = self.version,
        )
    }
}

impl fmt::Display for PackageId<MaybeVersion> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = self.registry.as_ref().map(|it| it.0.as_str()).unwrap_or("");
        let version = self
            .version
            .as_ref()
            .map(|it| format!(":{}", it))
            .unwrap_or_else(|| "".to_string());
        write!(
            f,
            "{registry}{group}/{name}:{version}",
            registry = registry,
            group = self.group.as_str(),
            name = self.name.as_str(),
            version = version,
        )
    }
}

impl<'de> Deserialize<'de> for PackageId<WithVersion> {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        let package_id = match string.parse::<PackageId<WithVersion>>() {
            Ok(pid) => pid,
            Err(e) => {
                use serde::de::{Unexpected, Error as DeserializeError};
                return Err(DeserializeError::invalid_value(
                    Unexpected::Other("Invalid PackageId"),
                    &&*format!("A PackageId, <registry>/<group>/<name>:<version>, where <registry> is optional: {}", e),
                ));
            }
        };
        Ok(package_id)
    }
}

impl<'de> Deserialize<'de> for PackageId<MaybeVersion> {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        let package_id = match string.parse::<PackageId<MaybeVersion>>() {
            Ok(pid) => pid,
            Err(e) => {
                use serde::de::{Unexpected, Error as DeserializeError};
                return Err(DeserializeError::invalid_value(
                    Unexpected::Other("Invalid PackageId"),
                    &&*format!("A PackageId, <registry>/<group>/<name>:<version>, where <registry> is optional: {}", e),
                ));
            }
        };
        Ok(package_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package_id::{Registry, PackageId};
    use semver::Version;

    #[test]
    fn test_deserialize_group() {
        let group: GroupName = "fluvio.io".parse().unwrap();

        #[derive(Debug, Deserialize, Serialize)]
        struct JsonGroup {
            group: GroupName,
        }

        let json_group = JsonGroup {
            group: group.clone(),
        };
        let group_string = serde_json::to_string(&json_group).unwrap();

        let read_json_group: JsonGroup = serde_json::from_str(&group_string).unwrap();
        let read_group = read_json_group.group;
        assert_eq!(group, read_group);

        assert!("fluvio/io".parse::<GroupName>().is_err());

        let bad_json_group = JsonGroup {
            group: GroupName("fluvio/io".to_string()),
        };
        let bad_group_string = serde_json::to_string(&bad_json_group).unwrap();
        let read_bad_group_result = serde_json::from_str::<JsonGroup>(&bad_group_string);
        assert!(read_bad_group_result.is_err());
    }

    #[test]
    fn test_parse_package_id_default_registry() {
        let package_id: PackageId<WithVersion> = "fluvio.io/fluvio:0.6.0".parse().unwrap();
        assert_eq!(package_id.registry(), &Registry::default());
        assert_eq!(package_id.group.as_str(), "fluvio.io");
        assert_eq!(package_id.name.as_str(), "fluvio");
        assert_eq!(package_id.version(), &Version::parse("0.6.0").unwrap());
    }

    #[test]
    fn test_parse_package_id_custom_registry() {
        let registry_url = "https://other.registry.io/v2/";
        let package_id: PackageId<WithVersion> = format!("{}/fluvio.io/fluvio:0.6.0", registry_url)
            .parse()
            .unwrap();
        assert_eq!(package_id.registry(), &registry_url.parse().unwrap());
        assert_eq!(package_id.group.as_str(), "fluvio.io");
        assert_eq!(package_id.name.as_str(), "fluvio");
        assert_eq!(package_id.version(), &Version::parse("0.6.0").unwrap());
    }

    #[test]
    fn test_package_id_idempotent() {
        let package_id = PackageId::new_versioned(
            "project-x-secret-sauce".parse().unwrap(),
            "infinyon.super.secret.division".parse().unwrap(),
            Version::parse("100.0.0-special-edition").unwrap(),
        );

        let package_id_string = package_id.to_string();
        assert_eq!(
            package_id_string,
            "infinyon.super.secret.division/\
            project-x-secret-sauce:100.0.0-special-edition"
        );

        let parsed_package_id: PackageId<WithVersion> = package_id_string.parse().unwrap();
        assert_eq!(package_id, parsed_package_id);
    }
}
