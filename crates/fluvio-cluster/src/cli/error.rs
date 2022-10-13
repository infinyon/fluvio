use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;
use fluvio_extension_common::target::TargetError;
use crate::check::ClusterCheckError;
use crate::{LocalInstallError};
use crate::ClusterError;

/// Cluster Command Error
#[derive(thiserror::Error, Debug)]
pub enum ClusterCliError {
    /// An IO error occurred, such as opening a file or running a command
    #[error(transparent)]
    IoError(#[from] IoError),
    /// Error printing command output
    #[error("Output Error")]
    OutputError(#[from] OutputError),
    /// Error building Fluvio configuration from CLI arguments
    #[error("Target Error")]
    TargetError(#[from] TargetError),
    /// An error occurred with a cluster operation
    #[error(transparent)]
    ClusterError(#[from] ClusterError),
    /// An error occurred while communicating with Fluvio
    #[error("Fluvio client error")]
    ClientError(#[from] FluvioError),
    /// Another type of error
    #[error("Unknown error: {0}")]
    Other(String),
    #[error(transparent)]
    ClusterCheckError(#[from] ClusterCheckError),
    #[error(transparent)]
    LocalInstallError(#[from] LocalInstallError),
}

impl ClusterCliError {
    /// Converts the plain error type into a CLI-formatted Report
    pub fn into_report(self) -> color_eyre::Report {
        use color_eyre::Report;

        match self {
            Self::ClusterError(cluster) => cluster.into_report(),
            _ => Report::from(self),
        }
    }
}

// This impl is here so that it is only compiled under "cli" feature flag
impl ClusterError {
    /// Converts the plain error type into a CLI-formatted Report
    pub fn into_report(self) -> color_eyre::Report {
        #[allow(unused)]
        use color_eyre::Section;
        use color_eyre::Report;
        use k8_client::ClientError as K8;

        // In the future when we want to annotate errors, we do it here
        match &self {
            Self::InstallLocal(LocalInstallError::K8ClientError(K8::ApiResponse(it)))
                if it.code == Some(409) =>
            {
                let report = Report::from(self);
                report.suggestion("Run `fluvio cluster delete --local`, then retry")
            }
            _ => Report::from(self),
        }
    }
}
