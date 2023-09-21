//! Fluvio Version Manager (FVM) Library
//!
//! Reusable components for the FVM CLI, constants and domain logic is
//! provided in this library crate.

pub mod common;
pub mod default;
pub mod install;
pub mod setup;
pub mod utils;

use std::fmt::Display;

use surf::StatusCode;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to find the Home Directory")]
    HomeDirNotFound,
    #[error("Failed to install a Fluvio Version. {0}")]
    Install(String),
    #[error("Setup failed. This might be related to an issue preparing FVM directory. {0}")]
    Setup(String),
    #[error("Failed to fetch HTTP Resource. {0}")]
    HttpError(HttpClientError),
    #[error("Failed to create temporal directory. {0}")]
    CreateTempDir(String),
    #[error("I/O Error. {0}")]
    IOError(#[from] std::io::Error),
}

/// Generic HTTP Client Error
#[derive(Debug, Error)]
pub struct HttpClientError {
    name: String,
    status: StatusCode,
}

impl Display for HttpClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Status Code: {}. {}",
            self.status,
            self.name,
        )
    }
}

impl From<surf::Error> for HttpClientError {
    fn from(value: surf::Error) -> Self {
        Self {
            name: value.to_string(),
            status: value.status(),
        }
    }
}

impl From<surf::Error> for Error {
    fn from(value: surf::Error) -> Self {
        let http_client_err = HttpClientError::from(value);

        Self::HttpError(http_client_err)
    }
}
