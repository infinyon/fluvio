pub mod topic;
pub mod spu;
pub mod spg;
pub mod connector;
pub mod smartmodule;
pub mod partition;
pub mod versions;
pub mod objects;
pub mod table;
mod apis;
mod request;
mod response;

pub use apis::AdminPublicApiKey;
pub use request::*;
pub use response::*;
pub use admin::*;

pub mod errors {
    pub use dataplane::ErrorCode;
}

pub use fluvio_controlplane_metadata::core;
pub use fluvio_controlplane_metadata::store;
pub use fluvio_controlplane_metadata::message;

/// Error from api call
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("Received error code: {0:#?} ({1:?})")]
    Code(dataplane::ErrorCode, Option<String>),
    #[error("No resource found: {0}")]
    NoResourceFound(String),
}

mod admin {

    use std::fmt::Debug;

    use dataplane::core::{Encoder, Decoder};
    use super::core::{Spec};

    /// filter by name
    pub type NameFilter = String;

    /// AdminSpec has list, type, filter, delete key
    pub trait AdminSpec: Spec + Encoder + Decoder {
        type ListFilter: Encoder + Decoder + Sized + Debug;
        type ListType: Encoder + Decoder + Debug;
        type WatchResponseType: Spec + Encoder + Decoder;
        type DeleteKey: Encoder + Decoder + Debug + Default + Sized;
    }
}
