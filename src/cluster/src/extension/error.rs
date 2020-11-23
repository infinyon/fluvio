use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;
use fluvio_extension_common::target::TargetError;
use fluvio_runner::RunnerError;

use crate::{ClusterError, CheckError};

pub type Result<T> = std::result::Result<T, ClusterCmdError>;

/// Cluster Command Error
#[derive(thiserror::Error, Debug)]
pub enum ClusterCmdError {
    #[error(transparent)]
    IoError {
        #[from]
        source: IoError,
    },
    #[error("Output Error")]
    OutputError {
        #[from]
        source: OutputError,
    },
    #[error("Target Error")]
    TargetError {
        #[from]
        source: TargetError,
    },
    #[error("Fluvio cluster error")]
    ClusterError {
        #[from]
        source: ClusterError,
    },
    #[error("Fluvio cluster pre install check error")]
    CheckError {
        #[from]
        source: CheckError,
    },
    #[error("Fluvio client error")]
    ClientError {
        #[from]
        source: FluvioError,
    },
    #[error("Runner error")]
    RunnerError {
        #[from]
        source: RunnerError
    },
    #[error("Unknown error: {0}")]
    Other(String),
}