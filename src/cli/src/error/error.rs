use std::fmt;
use std::io::Error as IoError;

use flv_client::ClientError;
use crate::profile::CloudError;

#[derive(Debug)]
pub enum CliError {
    InvalidArg(String),
    IoError(IoError),
    ClientError(ClientError),
    CloudError(CloudError),
    Other(String),
}

impl CliError {
    pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
        Self::InvalidArg(reason.into())
    }
}

impl From<IoError> for CliError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        Self::ClientError(error)
    }
}

impl From<CloudError> for CliError {
    fn from(error: CloudError) -> Self {
        Self::CloudError(error)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidArg(msg) => write!(f, "{}", msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::ClientError(err) => write!(f, "{}", err),
            Self::CloudError(err) => write!(f, "{}", err),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}
