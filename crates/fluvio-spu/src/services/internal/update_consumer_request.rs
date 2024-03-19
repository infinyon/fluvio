use std::fmt;
use std::time::Duration;

use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::Offset;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_spu_schema::COMMON_VERSION;
use fluvio_types::PartitionId;

use super::SPUPeerApiEnum;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateConsumerRequest {
    pub topic: String,
    pub partition: PartitionId,
    pub consumer_id: String,
    pub offset: Offset,
    pub ttl: Duration,
}

impl Request for UpdateConsumerRequest {
    const API_KEY: u16 = SPUPeerApiEnum::UpdateConsumer as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = UpdateConsumerResponse;
}

impl UpdateConsumerRequest {
    pub fn new(
        topic: impl Into<String>,
        partition: PartitionId,
        consumer_id: impl Into<String>,
        offset: Offset,
        ttl: Duration,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            consumer_id: consumer_id.into(),
            offset,
            ttl,
        }
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct UpdateConsumerResponse {
    pub error_code: ErrorCode,
}

impl fmt::Display for UpdateConsumerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error: {:#?}", self.error_code)
    }
}
