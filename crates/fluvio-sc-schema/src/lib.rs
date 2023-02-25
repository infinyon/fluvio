pub mod topic;
pub mod spu;
pub mod customspu;
pub mod spg;
pub mod smartmodule;
pub mod partition;
pub mod versions;
pub mod objects;
pub mod tableformat;

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
pub use error::ApiError;
mod error {

    use std::fmt::Display;
    use std::fmt::Formatter;

    use super::errors::ErrorCode;

    /// Error from api call
    #[derive(thiserror::Error, Debug)]
    pub enum ApiError {
        Code(crate::errors::ErrorCode, Option<String>),
        NoResourceFound(String),
    }

    impl Display for ApiError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                ApiError::Code(ErrorCode::TopicAlreadyExists, _) => {
                    write!(f, "Topic already exists")
                }
                ApiError::Code(ErrorCode::ManagedConnectorAlreadyExists, _) => {
                    write!(f, "Connector already exists")
                }
                ApiError::Code(ErrorCode::TopicNotFound, _) => {
                    write!(f, "Topic not found")
                }
                ApiError::Code(ErrorCode::SmartModuleNotFound { name: _ }, _) => {
                    write!(f, "SmartModule not found")
                }
                ApiError::Code(ErrorCode::ManagedConnectorNotFound, _) => {
                    write!(f, "Connector not found")
                }
                ApiError::Code(ErrorCode::TopicInvalidName, _) => {
                    write!(f,"Invalid topic name: topic name may only include lowercase letters (a-z), numbers (0-9), and hyphens (-).")
                }
                ApiError::Code(ErrorCode::TableFormatAlreadyExists, _) => {
                    write!(f, "TableFormat already exists")
                }
                ApiError::Code(ErrorCode::TableFormatNotFound, _) => {
                    write!(f, "TableFormat not found")
                }
                ApiError::Code(_, Some(msg)) => {
                    write!(f, "{msg}")
                }
                ApiError::Code(code, None) => {
                    write!(f, "{code}")
                }
                ApiError::NoResourceFound(name) => {
                    write!(f, "No resource found: {name}")
                }
            }
        }
    }
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
