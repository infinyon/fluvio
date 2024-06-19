use std::fmt::Debug;

use fluvio_stream_model::core::{Spec, Status};

use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_types::SpuId;

use crate::topic::TopicSpec;

#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct MirroringRemoteClusterRequest<S> {
    pub request: S,
}

impl<S> MirroringRemoteClusterSpec for MirroringRemoteClusterRequest<S> where S: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct MirrorConnect {
    pub remote_id: String,
}

impl MirroringRemoteClusterSpec for MirrorConnect {}

impl Spec for MirrorConnect {
    const LABEL: &'static str = "MirroringConnect";
    type IndexKey = String;
    type Status = MirroringStatus;
    type Owner = Self;
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MirroringStatus {}

impl std::fmt::Display for MirroringStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MirroringStatus")
    }
}

impl Status for MirroringStatus {}

impl Request for MirroringRemoteClusterRequest<MirrorConnect> {
    const API_KEY: u16 = MirroringApiKey::Connect as u16;
    type Response = MirroringStatusResponse;
}

/// API call from client to SPU
#[repr(u16)]
#[derive(Encoder, Decoder, Eq, PartialEq, Debug, Clone, Copy)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[fluvio(encode_discriminant)]
pub enum MirroringApiKey {
    ApiVersion = 1, // version api key
    Connect = 3000,
}

impl Default for MirroringApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}

pub trait MirroringRemoteClusterSpec: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct MirroringStatusResponse {
    pub name: String,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
    pub topics: Vec<MirroringSpecWrapper<TopicSpec>>,
}

impl MirroringStatusResponse {
    pub fn new_ok(name: &str, topics: Vec<MirroringSpecWrapper<TopicSpec>>) -> Self {
        MirroringStatusResponse {
            name: name.to_string(),
            error_code: ErrorCode::None,
            error_message: None,
            topics,
        }
    }
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct MirroringSpecWrapper<S> {
    pub key: String,
    pub spec: S,
    pub spu_id: SpuId,
    pub spu_key: String,
    pub spu_endpoint: String,
}

impl<S> MirroringSpecWrapper<S> {
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
