use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;
use fluvio_extension_common::target::TargetError;
use fluvio_runner_local::RunnerError;
use crate::ClusterError;

/// Cluster Command Error
#[derive(thiserror::Error, Debug)]
pub enum ClusterCliError {
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error("Output Error")]
    OutputError(#[from] OutputError),
    #[error("Target Error")]
    TargetError(#[from] TargetError),
    #[error("Fluvio cluster error")]
    ClusterError(#[from] ClusterError),
    #[error("Fluvio client error")]
    ClientError(#[from] FluvioError),
    #[error("Runner error")]
    RunnerError(#[from] RunnerError),
    #[error("Unknown error: {0}")]
    Other(String),
}

impl ClusterCliError {
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
    pub fn into_report(self) -> color_eyre::Report {
        #[allow(unused)]
        use color_eyre::Section;
        use color_eyre::Report;

        // In the future when we want to annotate errors, we do it here
        // match &self {
        //     _ => Report::from(self),
        // }
        Report::from(self)
    }
}
