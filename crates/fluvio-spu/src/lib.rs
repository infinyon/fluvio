mod error;
mod config;

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        mod core;
        mod services;
        mod start;
        mod replication;
        mod control_plane;
        mod storage;
       // mod smartengine;
        pub use start::main_loop;
    }
}

use self::error::InternalServerError;
pub use config::SpuOpt;

const VERSION: &str = include_str!("../../../VERSION");
