use crate::package_id::{GroupName, PackageName};
use crate::Target;

pub type Result<T> = std::result::Result<T, Error>;

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
    #[error("Invalid target {0}")]
    InvalidTarget(String),

    #[cfg(feature = "http_agent")]
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
    InvalidSemver(#[from] semver::Error),
    #[error("Invalid package name: {0}")]
    InvalidPackageName(String),
    #[error("Invalid group name: {0}")]
    InvalidGroupName(String),
    #[error("Invalid tag name: {0}")]
    InvalidTagName(String),
    #[error("Tag '{0}' does not exist")]
    TagDoesNotExist(String),
    #[error("Failed to parse PackageVersion from '{0}', must be valid Semver or Tag name")]
    InvalidPackageVersion(String),
    #[error("Version number is required here")]
    MissingVersion,
    #[error("Failed to parse registry segment of PackageId")]
    FailedToParseRegistry(url::ParseError),
    #[error("Failed to parse response: {0}")]
    InvalidData(#[from] serde_json::Error),
    #[error("An unknown error occurred: {0}")]
    Other(String),
}

#[cfg(feature = "http_agent")]
#[derive(thiserror::Error, Debug)]
#[error("Http error: {}", inner)]
pub struct HttpError {
    pub inner: http::Error,
}

#[cfg(feature = "http_agent")]
impl From<http::Error> for Error {
    fn from(inner: http::Error) -> Self {
        Self::HttpError(HttpError { inner })
    }
}
