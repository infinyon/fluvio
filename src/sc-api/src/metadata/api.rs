use std::io::Error as IoError;
use std::convert::TryInto;

use kf_protocol::bytes::Buf;
use kf_protocol::derive::*;
use kf_protocol::api::*;

use super::update_all::UpdateAllRequest;
use super::update_replica::ReplicaChangeRequest;
use super::update_spu::SpuChangeRequest;

/// API call from SC to client
#[derive(PartialEq, Debug, Encode, Decode, Clone, Copy)]
#[repr(u16)]
pub enum ScClientApiKey {
    UpdateAll = 1000,
    ReplicaChange = 1001,
    SpuChange = 1002,
}

impl Default for ScClientApiKey {
    fn default() -> Self {
        Self::UpdateAll
    }
}

/// Request going from SC to client
#[derive(Debug, Encode)]
pub enum ScClientRequest {
    /// Update all Replica, this happens at periodic time
    UpdateAllRequest(RequestMessage<UpdateAllRequest>),
    /// Update to change in replicas
    ReplicaChangeRequest(RequestMessage<ReplicaChangeRequest>),
    SpuChangeRequest(RequestMessage<SpuChangeRequest>),
}

// Added to satisfy Encode/Decode traits
impl Default for ScClientRequest {
    fn default() -> Self {
        Self::UpdateAllRequest(RequestMessage::default())
    }
}

impl KfRequestMessage for ScClientRequest {
    type ApiKey = ScClientApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        match header.api_key().try_into()? {
            ScClientApiKey::UpdateAll => api_decode!(Self, UpdateAllRequest, src, header),
            ScClientApiKey::ReplicaChange => api_decode!(Self, ReplicaChangeRequest, src, header),
            ScClientApiKey::SpuChange => api_decode!(Self, SpuChangeRequest, src, header),
        }
    }
}
