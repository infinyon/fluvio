use std::io::Error as IoError;

use fluvio::FluvioError;
use fluvio_extension_common::output::OutputError;

use crate::{ClusterError, CheckError};

pub type Result<T> = std::result::Result<T, ClusterCmdError>;

#[derive(thiserror::Error, Debug)]
pub enum ClusterCmdError {
    #[error(transparent)]
    IoError {
        #[from]
        source: IoError,
    },
    #[error(transparent)]
    OutputError {
        #[from]
        source: OutputError,
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
    #[error("Unknown error: {0}")]
    Other(String),
}