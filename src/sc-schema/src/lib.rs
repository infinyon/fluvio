pub mod topic;
pub mod spu;
pub mod spg;
pub mod partition;
pub mod versions;
pub mod objects;
mod request;
mod response;

pub use request::*;
pub use response::*;
pub use admin::*;

pub mod errors {
    pub use dataplane::ErrorCode;
}

pub use dataplane::apis::AdminPublicApiKey;
pub use fluvio_controlplane_metadata::core;
pub use fluvio_controlplane_metadata::store;

/// Error from api call
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("Received error code: {0:#?} ({1:?})")]
    Code(dataplane::ErrorCode, Option<String>),
    #[error("No resource found: {0}")]
    NoResourceFound(String),
}

mod admin {
    use dataplane::api::Request;

    pub trait AdminRequest: Request {}
}
