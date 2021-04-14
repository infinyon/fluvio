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
use once_cell::sync::Lazy;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = include_str!("../../../VERSION");
    match option_env!("FLUVIO_VERSION_SUFFIX") {
        Some(suffix) => format!("{}-{}", version, suffix),
        None => version.to_string(),
    }
});

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
