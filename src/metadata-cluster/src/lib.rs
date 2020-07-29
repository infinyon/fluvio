#![feature(drain_filter)]

pub mod spu;
pub mod topic;
pub mod partition;
pub mod spg;
pub mod message;

pub mod core {
    pub use flv_eventstream_model::core::*;
}

pub mod store {
    pub use flv_eventstream_model::store::*;
}

#[cfg(feature = "k8")]
pub mod k8 {

    pub mod core {
        pub use k8_obj_core::*;
    }

    pub mod app {
        pub use k8_obj_app::*;
    }

    pub mod metadata {
        pub use k8_obj_metadata::*;
    }
}
