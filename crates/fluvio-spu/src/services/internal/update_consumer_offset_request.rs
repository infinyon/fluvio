use std::fmt;

use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::{Offset, ReplicaKey};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_spu_schema::COMMON_VERSION;
use fluvio_types::PartitionId;

use super::SPUPeerApiEnum;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateConsumerOffsetRequest {
    pub replica_id: ReplicaKey,
    pub consumer_id: String,
    pub offset: Offset,
}

impl Request for UpdateConsumerOffsetRequest {
    const API_KEY: u16 = SPUPeerApiEnum::UpdateConsumerOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = UpdateConsumerOffsetResponse;
}

impl UpdateConsumerOffsetRequest {
    pub fn new(
        topic: impl Into<String>,
        partition: PartitionId,
        consumer_id: impl Into<String>,
        offset: Offset,
    ) -> Self {
        let replica_id = ReplicaKey::new(topic, partition);
        Self {
            replica_id,
            consumer_id: consumer_id.into(),
            offset,
        }
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct UpdateConsumerOffsetResponse {
    pub error_code: ErrorCode,
}

impl fmt::Display for UpdateConsumerOffsetResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error: {:#?}", self.error_code)
    }
}
