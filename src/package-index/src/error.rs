use crate::package_id::{GroupName, PackageName};
use crate::Target;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to lookup package: group {0} does not exist")]
    MissingGroup(GroupName),
    #[error("Failed to lookup package: package {0} does not exist")]
    MissingPackage(PackageName),
    #[error("Failed to lookup package: release version {0} does not exist")]
    MissingRelease(semver::Version),
    #[error("Failed to lookup package: target {0} does not exist")]
    MissingTarget(Target),
    #[error("Package {0} has no releases")]
    NoReleases(String),
    #[error("Failed to create new package {0}: it already exists")]
    PackageAlreadyExists(String),
    #[error("Failed to add release: release version {0} for {0} already exists")]
    ReleaseAlreadyExists(semver::Version, Target),
    #[error("Failed to parse URL")]
    UrlParseError(#[from] url::ParseError),
    #[error("Invalid platform {0}")]
    InvalidPlatform(String),
    #[error(transparent)]
    HttpError(#[from] HttpError),
    #[error("DANGER: Downloaded package checksum did not match")]
    ChecksumError,

    // Package ID specific errors
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
    FailedToParseRegistry(url::ParseError),
}

#[derive(thiserror::Error, Debug)]
#[error("Http error: {}", inner)]
pub struct HttpError {
    pub inner: http_types::Error,
}

impl From<http_types::Error> for Error {
    fn from(inner: http_types::Error) -> Self {
        Self::HttpError(HttpError { inner })
    }
}
