#[macro_use]
pub mod config;
pub mod k8;
pub mod cli;
pub mod core;
pub mod start;

pub mod stores;
pub mod metadata_store;
mod init;
mod error;
mod services;
mod controllers;

const VERSION: &str = include_str!("../../../VERSION");

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
