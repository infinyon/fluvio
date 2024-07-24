#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::api::Request;
use fluvio_protocol::derive::{Decoder, Encoder};
use fluvio_types::SpuId;

use super::SPUPeerApiEnum;

#[derive(Decoder, Encoder, Debug, Default)]
pub struct FetchStreamRequest {
    pub spu_id: SpuId,
    pub leader_spu_id: SpuId,
    pub min_bytes: i32,
    pub max_bytes: i32,
}

impl Request for FetchStreamRequest {
    const API_KEY: u16 = SPUPeerApiEnum::FetchStream as u16;
    type Response = FetchStreamResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchStreamResponse {
    pub spu_id: Option<SpuId>,
}

impl FetchStreamResponse {
    pub fn new(spu_id: Option<SpuId>) -> Self {
        FetchStreamResponse { spu_id }
    }
}
