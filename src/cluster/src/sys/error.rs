use semver::Error as VersionError;

use crate::charts::ChartInstallError;

/// Errors that may occur while trying to install Fluvio System Components
#[derive(thiserror::Error, Debug)]
pub enum SysInstallError {
    /// Attempted to construct a Config object without all required fields
    #[error("Missing required config option {0}")]
    MissingRequiredConfig(String),
    /// An error occurred while trying to install Fluvio system charts
    #[error("Chart Installation Error")]
    ChartError(#[from] ChartInstallError),
    #[error("Version Error")]
    VersionError(#[from] VersionError),
    #[error("Other System Error {0}")]
    Other(String),
}
