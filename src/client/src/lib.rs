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
        pub use flv_api_sc::topic::*;
    }

    pub mod spu {
        pub use flv_api_sc::spu::*;
    }

    pub mod spg {
        pub use flv_api_sc::spg::*;
    }

    pub mod partition {
        pub use flv_api_sc::partition::*;
    }

    pub mod objects {
        pub use flv_api_sc::objects::*;
    }

    pub mod core {
        pub use flv_api_sc::core::*;
    }

    pub mod store {
        pub use flv_api_sc::store::*;
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
