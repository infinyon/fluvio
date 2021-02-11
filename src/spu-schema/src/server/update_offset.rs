//!
//! # Update Offsets
//!

use dataplane::api::Request;
use dataplane::derive::Decode;
use dataplane::derive::Encode;
use dataplane::Offset;

use crate::errors::ErrorCode;
use super::SpuServerApiKey;

#[derive(Decode, Encode, Default, Debug)]
pub struct OffsetUpdate {
    pub offset: Offset,
    pub session_id: u32
}

/// send out current offset to SPU
#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateOffsetsRequest {
    pub offsets: Vec<OffsetUpdate>,
}

impl Request for UpdateOffsetsRequest {
    const API_KEY: u16 = SpuServerApiKey::UpdateOffsets as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = UpdateOffsetsResponse;
}

impl UpdateOffsetsRequest {
    pub fn new(offsets: Vec<OffsetUpdate>) -> Self {
        Self { offsets }
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct OffsetUpdateStatus {
    pub session_id: u32,
    pub error: ErrorCode,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct UpdateOffsetsResponse {
    pub status: Vec<OffsetUpdateStatus>,
}
