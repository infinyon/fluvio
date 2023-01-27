use semver::Version;
use fluvio_index::{PackageId, Target};

#[derive(thiserror::Error, Debug)]
#[error("Package {package} is not published at version {version} for target {target}")]
pub struct PackageNotFound {
    pub package: PackageId,
    pub version: Version,
    pub target: Target,
}

#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}
