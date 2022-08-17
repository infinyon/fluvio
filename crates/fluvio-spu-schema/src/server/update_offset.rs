//!
//! # Update Offsets
//!

use fluvio_protocol::api::Request;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::record::Offset;

use crate::errors::ErrorCode;
use super::SpuServerApiKey;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct OffsetUpdate {
    pub offset: Offset,
    pub session_id: u32,
}

/// send out current offset to SPU
#[derive(Decoder, Encoder, Default, Debug)]
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

#[derive(Decoder, Encoder, Default, Debug)]
pub struct OffsetUpdateStatus {
    pub session_id: u32,
    pub error: ErrorCode,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct UpdateOffsetsResponse {
    pub status: Vec<OffsetUpdateStatus>,
}
