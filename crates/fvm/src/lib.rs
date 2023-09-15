//! Fluvio Version Manager (FVM) Library
//!
//! Reusable components for the FVM CLI, constants and domain logic is
//! provided in this library crate.

pub mod constants;
pub mod setup;
pub mod utils;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to find the Home Directory")]
    HomeDirNotFound,
    #[error("Failed to initialize FVM. {0}")]
    InitFailed(String),
}
