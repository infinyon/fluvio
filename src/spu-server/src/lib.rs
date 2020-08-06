#![feature(generators)]
#![recursion_limit = "512"]
#![type_length_limit = "1101663"]
#![allow(clippy::assign_op_pattern)]

mod error;
mod start;
mod config;
mod core;
mod services;
mod controllers;

//#[cfg(test)]
//mod tests;

use self::error::InternalServerError;
pub use start::main_loop;
pub use config::SpuOpt;
