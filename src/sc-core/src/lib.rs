#![feature(trace_macros, generators,specialization)]
#![recursion_limit = "256"]

pub mod config;
mod conn_manager;
pub mod core;
mod init;
mod services;

pub mod metadata;
mod error;

pub use init::create_core_services;
pub use self::error::ScServerError;
pub use init::start_main_loop;
