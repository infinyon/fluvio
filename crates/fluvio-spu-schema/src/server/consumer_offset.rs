use fluvio_protocol::api::Request;
use fluvio_protocol::record::{Offset, ReplicaKey};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_types::PartitionId;

use crate::COMMON_VERSION;
use crate::errors::ErrorCode;
use super::SpuServerApiKey;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateConsumerOffsetRequest {
    pub offset: Offset,
    pub session_id: u32,
}

impl Request for UpdateConsumerOffsetRequest {
    const API_KEY: u16 = SpuServerApiKey::UpdateConsumerOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = UpdateConsumerOffsetResponse;
}

impl UpdateConsumerOffsetRequest {
    pub fn new(offset: Offset, session_id: u32) -> Self {
        Self { offset, session_id }
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct UpdateConsumerOffsetResponse {
    pub offset: Offset,
    pub error_code: ErrorCode,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct DeleteConsumerOffsetRequest {
    pub replica_id: ReplicaKey,
    pub consumer_id: String,
}

impl DeleteConsumerOffsetRequest {
    pub fn new(
        topic: impl Into<String>,
        partition: PartitionId,
        consumer_id: impl Into<String>,
    ) -> Self {
        let replica_id = ReplicaKey::new(topic, partition);
        Self {
            replica_id,
            consumer_id: consumer_id.into(),
        }
    }
}

impl Request for DeleteConsumerOffsetRequest {
    const API_KEY: u16 = SpuServerApiKey::DeleteConsumerOffset as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = DeleteConsumerOffsetResponse;
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct DeleteConsumerOffsetResponse {
    pub error_code: ErrorCode,
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct FetchConsumerOffsetsRequest;

impl Request for FetchConsumerOffsetsRequest {
    const API_KEY: u16 = SpuServerApiKey::FetchConsumerOffsets as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = FetchConsumerOffsetsResponse;
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct FetchConsumerOffsetsResponse {
    pub error_code: ErrorCode,
    pub consumers: Vec<ConsumerOffset>,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct ConsumerOffset {
    pub consumer_id: String,
    pub replica_id: ReplicaKey,
    pub offset: Offset,
    pub modified_time: u64,
}

impl ConsumerOffset {
    pub fn new(
        consumer_id: impl Into<String>,
        replica_id: impl Into<ReplicaKey>,
        offset: Offset,
        modified_time: u64,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            replica_id: replica_id.into(),
            offset,
            modified_time,
        }
    }
}
