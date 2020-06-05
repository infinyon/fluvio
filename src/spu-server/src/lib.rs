#![feature(generators)]
#![recursion_limit = "512"]
#![type_length_limit = "1101663"]

mod error;
mod start;
mod config;
mod core;
mod services;
mod controllers;

//#[cfg(test)]
//mod tests;

use start::main_loop;
use self::error::InternalServerError;

pub fn start_main() {
    flv_util::init_logger();
    main_loop();
}
