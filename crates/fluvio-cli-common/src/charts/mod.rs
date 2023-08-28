mod chart;
mod location;

pub use chart::*;
pub use error::*;
pub use location::*;

pub(crate) const SYS_CHART_NAME: &str = "fluvio-sys";
pub(crate) const APP_CHART_NAME: &str = "fluvio";
pub(crate) const DEFAULT_HELM_VERSION: &str = "3.3.4";

mod error {

    use std::io::Error as IoError;
    use fluvio_helm::HelmError;

    /// Errors that may occur while trying to install Fluvio system charts
    #[derive(thiserror::Error, Debug)]
    pub enum ChartInstallError {
        /// io error
        #[error(transparent)]
        IoError(#[from] IoError),
        /// An error occurred while running helm.
        #[error("Helm client error")]
        HelmError(#[from] HelmError),
        /// Attempted to construct a Config object without all required fields
        #[error("Missing required config option {0}")]
        MissingRequiredConfig(String),
        /// A different kind of error occurred.
        #[error("An unknown error occurred: {0}")]
        Other(String),
    }
}
