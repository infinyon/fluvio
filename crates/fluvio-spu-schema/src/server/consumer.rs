use std::time::Duration;

use fluvio_protocol::api::Request;
use fluvio_protocol::record::Offset;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_types::PartitionId;

use crate::COMMON_VERSION;
use crate::errors::ErrorCode;
use super::SpuServerApiKey;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateConsumerRequest {
    pub offset: Offset,
    pub session_id: u32,
}

impl Request for UpdateConsumerRequest {
    const API_KEY: u16 = SpuServerApiKey::UpdateConsumer as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = UpdateConsumerResponse;
}

impl UpdateConsumerRequest {
    pub fn new(offset: Offset, session_id: u32) -> Self {
        Self { offset, session_id }
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct UpdateConsumerResponse {
    pub offset: Offset,
    pub error_code: ErrorCode,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct DeleteConsumerRequest {
    pub topic: String,
    pub partition: PartitionId,
    pub consumer_id: String,
}

impl DeleteConsumerRequest {
    pub fn new(
        topic: impl Into<String>,
        partition: PartitionId,
        consumer_id: impl Into<String>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            consumer_id: consumer_id.into(),
        }
    }
}

impl Request for DeleteConsumerRequest {
    const API_KEY: u16 = SpuServerApiKey::DeleteConsumer as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = DeleteConsumerResponse;
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct DeleteConsumerResponse {
    pub error_code: ErrorCode,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchConsumersRequest;

impl Request for FetchConsumersRequest {
    const API_KEY: u16 = SpuServerApiKey::FetchConsumers as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = FetchConsumersResponse;
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchConsumersResponse {
    pub error_code: ErrorCode,
    pub consumers: Vec<Consumer>,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct Consumer {
    pub id: String,
    pub ttl: Duration,
    pub expire_time: u64,
}
