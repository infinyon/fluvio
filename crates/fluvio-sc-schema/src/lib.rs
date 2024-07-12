pub mod topic;
pub mod spu;
pub mod customspu;
pub mod spg;
pub mod smartmodule;
pub mod partition;
pub mod versions;
pub mod objects;
pub mod shared;
pub mod tableformat;
pub mod mirror;
pub mod mirroring;

pub mod remote_file;

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
                ApiError::Code(ErrorCode::MirrorNotFound, _) => {
                    write!(f, "Mirror not found")
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

    use anyhow::Result;

    use fluvio_protocol::{Encoder, Decoder, Version};
    use fluvio_controlplane_metadata::store::MetadataStoreObject;

    use crate::objects::Metadata;
    use crate::objects::classic::ClassicCreatableAdminSpec;

    use super::core::Spec;

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
    pub trait CreatableAdminSpec: ClassicCreatableAdminSpec + Spec + Encoder + Decoder {}

    pub trait DeletableAdminSpec: Spec + Encoder + Decoder {
        type DeleteKey: Encoder + Decoder + Debug + Default;
    }

    pub trait UpdatableAdminSpec: Spec + Encoder + Decoder {
        type UpdateKey: Encoder + Decoder + Debug + Default;
        type UpdateAction: Encoder + Decoder + Debug + Default + Clone;
    }

    /// try to encode type object into dynamic type which can be downcast later
    pub trait TryEncodableFrom<T>: Sized + Encoder + Decoder {
        fn try_encode_from(value: T, version: Version) -> Result<Self>;

        fn downcast(&self) -> Result<Option<T>>;
    }
}
