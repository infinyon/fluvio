//! Fluvio Version Manager (FVM) Library
//!
//! Reusable components for the FVM CLI, constants and domain logic is
//! provided in this library crate.

pub mod common;
pub mod install;
pub mod setup;
pub mod utils;

use install::InstallTask;
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
    #[error("Failed to fetch registry during install. {1}")]
    RegistryFetch(InstallTask, surf::Error),
    #[error("Failed to download artifact. {1}")]
    ArtifactDownload(InstallTask, surf::Error),
    #[error("Failed to create temporal directory. {0}")]
    CreateTempDir(String),
}
