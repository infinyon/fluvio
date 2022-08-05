use std::{
    io::{Error as IoError},
};

use semver::Version;
use fluvio_index::{PackageId, Target};

pub type Result<T, E = CliError> = core::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] IoError),

    #[error("Package index error")]
    IndexError(#[from] fluvio_index::Error),
    #[error(transparent)]
    HttpError(#[from] HttpError),
    #[error("Package {package} is not published at version {version} for target {target}")]
    PackageNotFound {
        package: PackageId,
        version: Version,
        target: Target,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    #[error("Http transport error: {0}")]
    TransportError(#[from] isahc::Error),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}
