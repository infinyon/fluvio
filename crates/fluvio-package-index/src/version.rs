use semver::Version;
use crate::{TagName, Error};
use std::fmt;

/// A type representing a package version in a PackageId.
///
/// In a PackageId string like `fluvio/fluvio:1.2.3`, this
/// is the `1.2.3`.
///
/// A `PackageVersion` may be either a direct semver, or it may be
/// the name of a tag which can later be resolved to a semver by
/// looking up the tag in the package registry.
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum PackageVersion {
    Semver(Version),
    Tag(TagName),
}

impl std::str::FromStr for PackageVersion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // First, try to parse as semver::Version
        if let Ok(version) = Version::parse(s) {
            tracing::debug!("Parsed PackageVersion::Semver from '{}'", s);
            return Ok(Self::Semver(version));
        }

        // Try to parse as TagName
        if let Ok(tagname) = s.parse::<TagName>() {
            tracing::debug!("Parsed PackageVersion::Tag from '{}'", s);
            return Ok(Self::Tag(tagname));
        };

        Err(Error::InvalidPackageVersion(s.to_string()))
    }
}

impl From<Version> for PackageVersion {
    fn from(version: Version) -> Self {
        Self::Semver(version)
    }
}

impl From<TagName> for PackageVersion {
    fn from(tag: TagName) -> Self {
        Self::Tag(tag)
    }
}

impl fmt::Display for PackageVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Semver(sv) => sv.fmt(f),
            Self::Tag(tag) => tag.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_package_version_semver() {
        let package_version = "1.2.3-alpha.1".parse::<PackageVersion>().unwrap();
        match &package_version {
            PackageVersion::Semver(sv) => {
                assert_eq!(sv.major, 1);
                assert_eq!(sv.minor, 2);
                assert_eq!(sv.patch, 3);
                assert_eq!(sv.pre.as_str(), "alpha.1");
                assert_eq!(format!("{package_version}"), "1.2.3-alpha.1");
            }
            _ => panic!("should have parsed as semver"),
        }
    }

    #[test]
    fn test_package_version_tag() {
        let package_version = "stable".parse::<PackageVersion>().unwrap();
        match &package_version {
            PackageVersion::Tag(tag) => {
                assert_eq!(tag.as_ref(), "stable");
                assert_eq!(format!("{package_version}"), "stable");
            }
            _ => panic!("should have parsed as tag"),
        }
    }
}
