use std::fmt;
use std::io::Error as IoError;

use flv_client::ClientError;
use kf_socket::KfSocketError;

#[derive(Debug)]
pub enum CliError {
    IoError(IoError),
    KfSocketError(KfSocketError),
    ClientError(ClientError),
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
            CliError::IoError(err) => write!(f, "{}", err),
            CliError::KfSocketError(err) => write!(f, "{}", err),
            CliError::ClientError(err) => write!(f, "{}", err),
        }
    }
}
