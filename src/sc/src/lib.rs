#![type_length_limit = "1337117"]

#[cfg(feature = "k8")]
pub mod k8;
pub mod cli;
pub mod core;
pub mod config;
pub mod stores;
mod init;
mod error;
mod services;
mod controllers;

pub use init::start_main_loop;

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
