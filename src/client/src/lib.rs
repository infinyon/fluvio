mod error;
mod client;
mod admin;
mod consumer;
mod producer;
mod sync;
mod spu;

pub mod config;
pub mod params;

pub use error::ClientError;
pub use config::ClusterConfig;
pub use producer::Producer;
pub use consumer::Consumer;

/// re-export metadata from controlplane-api
pub mod metadata {

    pub mod topic {
        pub use fluvio_controlplane_api::topic::*;
    }

    pub mod spu {
        pub use fluvio_controlplane_api::spu::*;
    }

    pub mod spg {
        pub use fluvio_controlplane_api::spg::*;
    }

    pub mod partition {
        pub use fluvio_controlplane_api::partition::*;
    }

    pub mod objects {
        pub use fluvio_controlplane_api::objects::*;
    }

    pub mod core {
        pub use fluvio_controlplane_api::core::*;
    }

    pub mod store {
        pub use fluvio_controlplane_api::store::*;
    }
}

pub mod kf {
    pub mod api {
        pub use kf_protocol::api::*;
    }

    pub mod message {
        pub use kf_protocol::message::*;
    }
}
