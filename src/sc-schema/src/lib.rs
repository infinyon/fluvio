mod apis;
pub mod topic;
pub mod spu;
pub mod spg;
pub mod partition;
pub mod versions;
pub mod objects;
mod request;
mod response;

use thiserror::Error;
pub use apis::*;
pub use request::*;
pub use response::*;
pub use admin::*;

pub mod errors {
    pub use dataplane::ErrorCode;
}

pub mod core {
    pub use fluvio_controlplane_metadata::core::*;
}

pub mod store {
    pub use fluvio_controlplane_metadata::store::*;
}

/// Error from api call
#[derive(Error, Debug)]
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
