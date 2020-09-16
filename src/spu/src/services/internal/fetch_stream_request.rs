#![allow(clippy::assign_op_pattern)]

use dataplane_protocol::api::Request;
use dataplane_protocol::derive::{Decode, Encode};
use fluvio_types::SpuId;

use super::SPUPeerApiEnum;

#[derive(Decode, Encode, Debug, Default)]
pub struct FetchStreamRequest {
    pub spu_id: SpuId,
    pub min_bytes: i32,
    pub max_bytes: i32,
}

impl Request for FetchStreamRequest {
    const API_KEY: u16 = SPUPeerApiEnum::FetchStream as u16;
    type Response = FetchStreamResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct FetchStreamResponse {
    pub spu_id: SpuId,
}

impl FetchStreamResponse {
    pub fn new(spu_id: SpuId) -> Self {
        FetchStreamResponse { spu_id }
    }
}
