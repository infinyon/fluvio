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
#[error("Http Error: {}", inner)]
pub struct HttpError {
    pub(crate) inner: http_types::Error,
}

impl From<http_types::Error> for CliError {
    fn from(inner: http_types::Error) -> Self {
        Self::HttpError(HttpError { inner })
    }
}
