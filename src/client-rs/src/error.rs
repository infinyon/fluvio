
use std::fmt;
use std::io::Error as IoError;

use kf_socket::KfSocketError;
use sc_api::ApiError;

#[derive(Debug)]
pub enum ClientError {
    TopicNotFound(String),      
    PartitionNotFound(String,i32),  
    Other(String),
    IoError(IoError),
    KfSocketError(KfSocketError),
    ScApiError(ApiError),
    UnableToReadProfile
}

impl From<IoError> for ClientError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<KfSocketError> for ClientError {
    fn from(error: KfSocketError) -> Self {
        Self::KfSocketError(error)
    }
}

impl From<ApiError> for ClientError {
    fn from(error: ApiError) -> Self {
        Self::ScApiError(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TopicNotFound(topic) => write!(f,"topic: {} not found",topic),
            Self::PartitionNotFound(topic,partition) => write!(f,"partition <{}:{}> not found",topic,partition),
            Self::Other(msg) => write!(f,"{}",msg),
            Self::IoError(err) => write!(f, "{}", err),
            Self::KfSocketError(err) => write!(f,"{:#?}",err),
            Self::ScApiError(err) => write!(f,"{}",err),
            Self::UnableToReadProfile => write!(f,"No configuration has been provided")
        }
    }
}
