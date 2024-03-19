use std::fmt;
use std::time::Duration;

use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::{Encoder, Decoder};

use fluvio_spu_schema::COMMON_VERSION;
use fluvio_types::PartitionId;

use super::SPUPeerApiEnum;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchConsumerRequest {
    pub topic: String,
    pub partition: PartitionId,
    pub consumer_id: String,
}

impl Request for FetchConsumerRequest {
    const API_KEY: u16 = SPUPeerApiEnum::FetchConsumer as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = FetchConsumerResponse;
}

impl FetchConsumerRequest {
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

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchConsumerResponse {
    pub error_code: ErrorCode,
    pub consumer: Option<Consumer>,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct Consumer {
    pub offset: i64,
    pub ttl: Duration,
}

impl FetchConsumerResponse {
    pub fn new(error_code: ErrorCode, consumer: Option<Consumer>) -> Self {
        Self {
            error_code,
            consumer,
        }
    }
}

impl Consumer {
    pub fn new(offset: i64, ttl: Duration) -> Self {
        Self { offset, ttl }
    }
}

impl fmt::Display for FetchConsumerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "error: {:#?}, consumer: {:?}",
            self.error_code, self.consumer
        )
    }
}
