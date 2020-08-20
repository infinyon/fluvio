#![allow(clippy::assign_op_pattern)]

use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_types::SpuId;

use super::KfSPUPeerApiEnum;

#[derive(Decode, Encode, Debug, Default)]
pub struct FetchStreamRequest {
    pub spu_id: SpuId,
    pub min_bytes: i32,
    pub max_bytes: i32,
}

impl Request for FetchStreamRequest {
    const API_KEY: u16 = KfSPUPeerApiEnum::FetchStream as u16;
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
