use std::fmt;

use kf_socket::KfSocketError;
use std::io::Error as IoError;

#[derive(Debug)]
pub enum CliError {
    IoError(IoError),
    KfSocketError(KfSocketError),
}

impl From<IoError> for CliError {
    fn from(error: IoError) -> Self {
        CliError::IoError(error)
    }
}

impl From<KfSocketError> for CliError {
    fn from(error: KfSocketError) -> Self {
        CliError::KfSocketError(error)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CliError::IoError(err) => write!(f, "{}", err),
            CliError::KfSocketError(err) => write!(f, "{}", err),
        }
    }
}