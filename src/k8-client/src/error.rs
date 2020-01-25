use std::io::Error as IoError;
use std::env;
use std::fmt;

use http;
use http::header::InvalidHeaderValue;
use isahc::Error as HttpError;

use k8_diff::DiffError;
use k8_config::ConfigError;

use k8_metadata::client::MetadataClientError;

// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html

#[derive(Debug)]
pub enum ClientError {
    IoError(IoError),
    HttpError(http::Error),
    InvalidHttpHeader(InvalidHeaderValue),
    EnvError(env::VarError),
    JsonError(serde_json::Error),
    DiffError(DiffError),
    HttpClientError(HttpError),
    K8ConfigError(ConfigError),
    PatchError,
    NotFound,
}

impl From<InvalidHeaderValue> for ClientError {
    fn from(error: InvalidHeaderValue) -> Self {
        Self::InvalidHttpHeader(error)
    }
}

impl From<IoError> for ClientError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<http::Error> for ClientError {
    fn from(error: http::Error) -> Self {
        Self::HttpError(error)
    }
}

impl From<env::VarError> for ClientError {
    fn from(error: env::VarError) -> Self {
        Self::EnvError(error)
    }
}


impl From<serde_json::Error> for ClientError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonError(error)
    }
}

impl From<DiffError> for ClientError {
    fn from(error: DiffError) -> Self {
        Self::DiffError(error)
    }
}

impl From<HttpError> for ClientError {
    fn from(error: HttpError) -> Self {
        Self::HttpClientError(error)
    }
}

impl From<ConfigError> for ClientError {
    fn from(error: ConfigError) -> Self {
        Self::K8ConfigError(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::HttpError(err) => write!(f, "{}", err),
            Self::EnvError(err) => write!(f, "{}", err),
            Self::JsonError(err) => write!(f, "{}", err),
            Self::NotFound => write!(f, "not found"),
            Self::DiffError(err) => write!(f, "{:#?}", err),
            Self::PatchError => write!(f, "patch error"),
            Self::HttpClientError(err) => write!(f,"{}",err),
            Self::K8ConfigError(err) => write!(f,"{}",err),
            Self::InvalidHttpHeader(err) => write!(f,"{}",err)
        }
    }
}

impl MetadataClientError for ClientError {

    fn patch_error() -> Self {
        Self::PatchError
    }

    fn not_founded(&self) -> bool {
        match self {
            Self::NotFound => true,
            _ => false
        }
    }    

}