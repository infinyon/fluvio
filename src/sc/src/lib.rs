#[cfg(any(feature = "k8"))]
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

cfg_if::cfg_if! {
    if #[cfg(feature = "platform")] {
        const VERSION: &str = include_str!("../../../VERSION");
    } else {
        const VERSION: &str = "UNDEFINED";
    }
}

pub mod dispatcher {
    pub use fluvio_stream_dispatcher::*;
}
