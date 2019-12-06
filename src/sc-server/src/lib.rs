#![feature(trace_macros, generators,specialization)]
#![recursion_limit = "256"]

mod cli;
mod conn_manager;
mod core;
//mod hc_manager;
mod init;
mod services;
mod k8;
mod error;

//#[cfg(test)]
//mod tests;
pub use init::create_core_services;
pub use self::error::ScServerError;

// start controller services
pub fn start_main() {
    utils::init_logger();
    init::main_loop();
}
