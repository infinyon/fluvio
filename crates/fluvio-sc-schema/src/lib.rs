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

    use dataplane::api::Request;
    use dataplane::core::{Encoder, Decoder};
    use super::core::{Spec,Status};

    pub trait ListFilter: Encoder + Decoder + Sized + Debug {}
    /// filter by name
    pub type NameFilter = String;

    impl ListFilter for NameFilter {}

    /// AdminSpec has list, type, filter, delete key
    pub trait AdminSpec: Spec + Encoder + Decoder {
        type ListFilter: ListFilter;
        type ListType: Encoder + Decoder + Debug + Sized;
        type WatchResponseType: Encoder + Decoder + Debug + Sized;
        type DeleteKey: Encoder + Decoder + Debug + Sized;
    }

    pub trait AdminRequest: Request {}

    /// dummy implementation to get non generic API constants such as API Key
    #[derive(Debug, Clone, PartialEq, Encoder, Default, Decoder)]
    pub struct AnySpec{}

    impl Spec for AnySpec {
        const LABEL: &'static str = "Any";

        type Status = AnyStatus;

        type Owner = Self;

        type IndexKey = String;
    }

    #[derive(Debug, Clone, PartialEq, Encoder, Default, Decoder)]
    pub struct AnyStatus{}

    impl Status for AnyStatus {

    }
    

    impl AdminSpec for AnySpec {
        type ListFilter = NameFilter;
        type ListType = crate::objects::MetadataUpdate<Self>;
        type WatchResponseType = crate::objects::MetadataUpdate<Self>;
        type DeleteKey = String;
    }
}
