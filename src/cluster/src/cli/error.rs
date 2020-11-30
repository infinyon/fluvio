use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;
use fluvio_extension_common::target::TargetError;
use fluvio_runner_local::RunnerError;
use crate::{ClusterError, CheckError};

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
    #[error("Fluvio cluster pre install check error")]
    CheckError(#[from] CheckError),
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
        use color_eyre::{Report, Section};
        use fluvio_helm::HelmError as Helm;
        use CheckError as Check;
        use crate::K8InstallError::*;

        match &self {
            Self::InstallK8(PreCheck(Check::HelmError(Helm::FailedToConnect))) => {
                let report = Report::from(self);
                #[cfg(target_os = "macos")]
                let report =
                    report.suggestion("Make sure you have run 'minikube start --driver=hyperkit'");
                #[cfg(not(target_os = "macos"))]
                let report =
                    report.suggestion("Make sure you have run 'minikube start --driver=docker'");
                report
            }
            Self::InstallK8(PreCheck(Check::MinikubeTunnelNotFound)) => {
                let report = Report::from(self);
                let report = report.suggestion("Make sure minikube tunnel is running");
                report.suggestion("Run 'minikube tunnel >/tmp/tunnel.out 2>/tmp/tunnel.out'")
            }
            _ => Report::from(self),
        }
    }
}
