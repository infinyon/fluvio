use std::io::Error as IoError;

use http;
use hyper;

use std::env;
use std::fmt;
use k8_diff::DiffError;

// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html

#[derive(Debug)]
pub enum ClientError {
    IoError(IoError),
    HttpError(http::Error),
    EnvError(env::VarError),
    HyperError(hyper::Error),
    JsonError(serde_json::Error),
    DiffError(DiffError),
    PatchError,
    NotFound,
}

impl From<IoError> for ClientError {
    fn from(error: IoError) -> Self {
        ClientError::IoError(error)
    }
}

impl From<http::Error> for ClientError {
    fn from(error: http::Error) -> Self {
        ClientError::HttpError(error)
    }
}

impl From<env::VarError> for ClientError {
    fn from(error: env::VarError) -> Self {
        ClientError::EnvError(error)
    }
}

impl From<hyper::Error> for ClientError {
    fn from(error: hyper::Error) -> Self {
        ClientError::HyperError(error)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(error: serde_json::Error) -> ClientError {
        ClientError::JsonError(error)
    }
}

impl From<DiffError> for ClientError {
    fn from(error: DiffError) -> Self {
        ClientError::DiffError(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClientError::IoError(err) => write!(f, "{}", err),
            ClientError::HttpError(err) => write!(f, "{}", err),
            ClientError::EnvError(err) => write!(f, "{}", err),
            ClientError::HyperError(err) => write!(f, "{}", err),
            ClientError::JsonError(err) => write!(f, "{}", err),
            ClientError::NotFound => write!(f, "not found"),
            ClientError::DiffError(err) => write!(f, "{:#?}", err),
            ClientError::PatchError => write!(f, "patch error"),
        }
    }
}
