#[macro_use]
pub mod config;
#[cfg(feature = "k8")]
pub mod k8;
pub mod cli;
pub mod core;

pub mod stores;
mod init;
mod error;
mod services;
mod controllers;

pub use init::start_main_loop;

const VERSION: &str = include_str!("../../../VERSION");

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
