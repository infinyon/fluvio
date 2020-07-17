#![recursion_limit = "256"]

pub mod config;
pub mod core;
mod controllers;
pub mod metadata;
mod error;
mod init;
mod services;
pub mod stores;


use self::error::ScServerError;
pub use init::start_main_loop;
