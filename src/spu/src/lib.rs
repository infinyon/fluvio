mod error;
mod config;

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        mod core;
        mod services;
        mod start;
        mod controllers;
        mod smart_stream;
        mod liveness_check;

        pub use start::main_loop;
        pub use liveness_check::probe;
    }
}

use self::error::InternalServerError;
pub use config::SpuOpt;
