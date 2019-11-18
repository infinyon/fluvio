use std::io::Error as IoError;
use std::env;
use std::fmt;

use http;
use isahc::Error as HttpError;

use k8_diff::DiffError;
use k8_config::ConfigError;

// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html

#[derive(Debug)]
pub enum ClientError {
    IoError(IoError),
    HttpError(http::Error),
    EnvError(env::VarError),
    JsonError(serde_json::Error),
    DiffError(DiffError),
    HttpClientError(HttpError),
    K8ConfigError(ConfigError),
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

impl From<HttpError> for ClientError {
    fn from(error: HttpError) -> Self {
        ClientError::HttpClientError(error)
    }
}

impl From<ConfigError> for ClientError {
    fn from(error: ConfigError) -> Self {
        ClientError::K8ConfigError(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClientError::IoError(err) => write!(f, "{}", err),
            ClientError::HttpError(err) => write!(f, "{}", err),
            ClientError::EnvError(err) => write!(f, "{}", err),
            ClientError::JsonError(err) => write!(f, "{}", err),
            ClientError::NotFound => write!(f, "not found"),
            ClientError::DiffError(err) => write!(f, "{:#?}", err),
            ClientError::PatchError => write!(f, "patch error"),
            ClientError::HttpClientError(err) => write!(f,"{}",err),
            ClientError::K8ConfigError(err) => write!(f,"{}",err)
        }
    }
}
