#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::fmt::Display;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Error as IoError;
use std::io::ErrorKind;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use fluvio_controlplane_metadata::core::MetadataContext;
use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::core::Spec;
use fluvio_controlplane_metadata::store::MetadataStoreObject;
use fluvio_protocol::bytes::Bytes;




use crate::AdminPublicApiKey;
use crate::AdminRequest;

/// marker trait
pub trait ListFilter {}

/// filter by name
pub type NameFilter = String;

impl ListFilter for NameFilter {}

pub trait ListSpec: Spec {
    /// filter type
    type Filter: ListFilter;

    type ListType: Encoder + Decoder;

    /// convert to list request with filters
    #[allow(clippy::wrong_self_convention)]
    fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest;
}

#[derive(Debug,Default,Encoder,Decoder)]
pub struct ListRequest {
    object_type: String,
    name_filters: Vec<NameFilter>
}



impl Request for ListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ListResponse;
}

impl AdminRequest for ListRequest {}



#[derive(Debug,Default,Encoder,Decoder)]
pub struct ListResponse {
    pub object_type: String,
    buffer: Bytes,
}


#[derive(Encoder, Decoder, Default, Clone, Debug)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Metadata<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
{
    pub name: String,
    pub spec: S,
    pub status: S::Status,
}

impl<S, C> From<MetadataStoreObject<S, C>> for Metadata<S>
where
    S: Spec + Encoder + Decoder,
    S::IndexKey: ToString,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
{
    fn from(meta: MetadataStoreObject<S, C>) -> Self {
        Self {
            name: meta.key.to_string(),
            spec: meta.spec,
            status: meta.status,
        }
    }
}

impl<S, C> TryFrom<Metadata<S>> for MetadataStoreObject<S, C>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
    <S as Spec>::IndexKey: TryFrom<String>,
    <<S as Spec>::IndexKey as TryFrom<String>>::Error: Display,
{
    type Error = IoError;

    fn try_from(value: Metadata<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            spec: value.spec,
            status: value.status,
            key: value.name.try_into().map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("problem converting: {}", err),
                )
            })?,
            ctx: MetadataContext::default(),
        })
    }
}