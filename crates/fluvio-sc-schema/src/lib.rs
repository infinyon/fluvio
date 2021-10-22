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
    use super::core::Spec;


    pub trait ListFilter: Encoder + Decoder + Sized  + Debug {

    }
    /// filter by name
    pub type NameFilter = String;

    impl ListFilter for NameFilter {}

    pub trait ListType: Encoder + Decoder + Sized{
    }

    pub trait DeleteKey: Encoder + Decoder + Sized{
    }


    /// Admin spec, they are 
    pub trait AdminSpec: Spec +  Encoder + Decoder {

        const AdminType: u8;

        /// filter type
        type ListFilter: ListFilter;
        type ListType: ListType;
        type DeleteKey: DeleteKey; 

    }


    pub trait AdminRequest: Request {}
}



