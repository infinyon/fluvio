pub mod topic;
pub mod spu;
pub mod customspu;
pub mod spg;
pub mod connector;
pub mod smartmodule;
pub mod partition;
pub mod versions;
pub mod objects;
pub mod tableformat;
pub mod derivedstream;

mod apis;
mod request;
mod response;

pub use apis::AdminPublicApiKey;
pub use request::*;
pub use response::*;
pub use admin::*;

pub mod errors {
    pub use fluvio_protocol::link::ErrorCode;
}

pub use fluvio_controlplane_metadata::core;
pub use fluvio_controlplane_metadata::store;
pub use fluvio_controlplane_metadata::message;

/// Error from api call
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("Received error code: {0:#?} ({1:?})")]
    Code(crate::errors::ErrorCode, Option<String>),
    #[error("No resource found: {0}")]
    NoResourceFound(String),
}

mod admin {

    use std::fmt::Debug;

    use fluvio_protocol::{Encoder, Decoder};
    use fluvio_controlplane_metadata::{store::MetadataStoreObject};

    use crate::objects::Metadata;

    use super::core::{Spec};

    /// AdminSpec can perform list and watch
    pub trait AdminSpec: Spec + Encoder + Decoder {
        /// convert metadata object to list type object
        fn convert_from<C: fluvio_controlplane_metadata::core::MetadataItem>(
            obj: &fluvio_controlplane_metadata::store::MetadataStoreObject<Self, C>,
        ) -> Metadata<Self>
        where
            Metadata<Self>: From<MetadataStoreObject<Self, C>>,
            Self::Status: Encoder + Decoder + Debug,
        {
            obj.clone().into()
        }

        /// return summary version of myself
        fn summary(self) -> Self {
            self
        }
    }

    /// Not every Admin Object can be created directly
    pub trait CreatableAdminSpec: Spec + Encoder + Decoder {
        const CREATE_TYPE: u8;
    }

    pub trait DeletableAdminSpec: Spec + Encoder + Decoder {
        type DeleteKey: Encoder + Decoder + Debug + Default;
    }
}
