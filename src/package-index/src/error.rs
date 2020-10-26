use crate::package_id::{GroupName, PackageName};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to parse PackageId")]
    PackageIdParseError(#[from] PackageIdError),
    #[error("Failed to lookup package: group {0} does not exist")]
    MissingGroup(GroupName),
    #[error("Failed to lookup package: package {0} does not exist")]
    MissingPackage(PackageName),
    #[error("Failed to lookup package: release version {0} does not exist")]
    MissingRelease(semver::Version),
    #[error("Failed to create new package {0}: it already exists")]
    PackageAlreadyExists(String),
    #[error("Failed to add release: release version {0} already exists")]
    ReleaseAlreadyExists(semver::Version),
}

#[derive(thiserror::Error, Debug)]
pub enum PackageIdError {
    #[error("PackageIds must have at least one `/` separator: <group>/<name>:<version>")]
    TooFewSlashes,
    #[error("PackageIds must have zero or one `:` separator: <name>(:<version>)?")]
    InvalidNameVersionSegment,
    #[error("Invalid semver")]
    InvalidSemver(#[from] semver::SemVerError),
    #[error("Invalid package name: {0}")]
    InvalidPackageName(String),
    #[error("Invalid group name: {0}")]
    InvalidGroupName(String),
    #[error("Version number is required here")]
    MissingVersion,
    #[error("Failed to parse registry segment of PackageId")]
    FailedToParseRegistry(#[from] url::ParseError),
}
