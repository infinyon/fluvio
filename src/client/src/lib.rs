mod error;
mod client;
mod admin;
mod consumer;
mod producer;
mod sync;
pub mod spu;

pub mod config;
pub mod params;

pub use error::ClientError;
pub use client::ClusterSocket;
pub use config::ClusterConfig;
pub use producer::Producer;
pub use consumer::Consumer;

/// re-export metadata from sc-api
pub mod metadata {

    pub mod topic {
        pub use fluvio_sc_schema::topic::*;
    }

    pub mod spu {
        pub use fluvio_sc_schema::spu::*;
    }

    pub mod spg {
        pub use fluvio_sc_schema::spg::*;
    }

    pub mod partition {
        pub use fluvio_sc_schema::partition::*;
    }

    pub mod objects {
        pub use fluvio_sc_schema::objects::*;
    }

    pub mod core {
        pub use fluvio_sc_schema::core::*;
    }

    pub mod store {
        pub use fluvio_sc_schema::store::*;
    }
}

pub mod dataplane {
    pub use dataplane::*;
}
