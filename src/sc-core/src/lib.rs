#![recursion_limit = "256"]

pub mod config;
pub mod core;
mod controllers;
mod error;
mod init;
mod services;
pub mod stores;

pub use init::start_main_loop;

pub mod dispatcher {
    pub use flv_eventstream_dispatcher::*;
}
