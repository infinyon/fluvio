use std::fmt;
use std::io::Error as IoError;

use flv_client::ClientError;
use kf_socket::KfSocketError;

#[derive(Debug)]
pub enum CliError {
    InvalidArg(String),
    IoError(IoError),
    KfSocketError(KfSocketError),
    ClientError(ClientError),
    Other(String),
}

impl From<IoError> for CliError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<KfSocketError> for CliError {
    fn from(error: KfSocketError) -> Self {
        Self::KfSocketError(error)
    }
}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        Self::ClientError(error)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidArg(msg) => write!(f, "{}", msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::KfSocketError(err) => write!(f, "{}", err),
            Self::ClientError(err) => write!(f, "{}", err),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}
