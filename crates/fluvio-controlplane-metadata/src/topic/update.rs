use std::fmt::Debug;

use fluvio_stream_model::core::{Spec, Status};

use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_types::SpuId;

#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct TopicUpdateRequest<S> {
    pub request: S,
}

impl<S> TopicUpdateSpec for TopicUpdateRequest<S> where S: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct AddPartition {
    pub topic: String,
    pub number_of_partition: u32,
}

impl TopicUpdateSpec for AddPartition {}

impl Spec for AddPartition {
    const LABEL: &'static str = "TopicUpdateAddPartition";
    type IndexKey = String;
    type Status = TopicUpdateStatus;
    type Owner = Self;
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TopicUpdateStatus {}

impl std::fmt::Display for TopicUpdateStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TopicUpdateStatus")
    }
}

impl Status for TopicUpdateStatus {}

impl Request for TopicUpdateRequest<AddPartition> {
    const API_KEY: u16 = TopicUpdateApiKey::AddPartition as u16;
    type Response = TopicUpdateStatusResponse;
}

/// API call from client to SPU
#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[fluvio(encode_discriminant)]
pub enum TopicUpdateApiKey {
    ApiVersion = 1, // version api key
    AddPartition = 4000,
}

impl Default for TopicUpdateApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}

pub trait TopicUpdateSpec: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicUpdateStatusResponse {
    pub name: String,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
}

impl TopicUpdateStatusResponse {
    pub fn new_ok(name: &str) -> Self {
        TopicUpdateStatusResponse {
            name: name.to_string(),
            error_code: ErrorCode::None,
            error_message: None,
        }
    }
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicUpdateSpecWrapper<S> {
    pub key: String,
    pub spec: S,
    pub spu_id: SpuId,
    pub spu_key: String,
    pub spu_endpoint: String,
}

impl<S> TopicUpdateSpecWrapper<S> {
    pub fn new(key: String, spec: S, spu_id: i32, spu_endpoint: String, spu_key: String) -> Self {
        Self {
            key,
            spec,
            spu_id,
            spu_endpoint,
            spu_key,
        }
    }
}
