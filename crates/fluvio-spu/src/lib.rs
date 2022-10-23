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
        mod smartengine;
        mod monitoring;
        pub use start::main_loop;
    }
}

use self::error::InternalServerError;
pub use config::SpuOpt;

const VERSION: &str = include_str!("../../../VERSION");

pub(crate) mod traffic {
    use fluvio_protocol::api::RequestHeader;

    pub(crate) trait TrafficType {
        // check if traffic is connector
        fn is_connector(&self) -> bool;
    }

    impl TrafficType for RequestHeader {
        fn is_connector(&self) -> bool {
            self.client_id().starts_with("fluvio_connector")
        }
    }
}
