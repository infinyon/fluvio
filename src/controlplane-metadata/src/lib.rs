#![feature(drain_filter)]

pub mod spu;
pub mod topic;
pub mod partition;
pub mod spg;
pub mod message;

pub mod core {
    pub use fluvio_stream_model::core::*;
}

pub mod store {
    pub use fluvio_stream_model::store::*;
}

#[cfg(feature = "k8")]
pub mod k8 {

    pub use fluvio_stream_model::k8::*;
}
