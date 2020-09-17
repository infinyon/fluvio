use dataplane::api::Request;
use dataplane::derive::Decode;
use dataplane::derive::Encode;
use dataplane::ReplicaKey;
use dataplane::Offset;
use dataplane::ErrorCode;

use super::SpuClientApiKey;

// -----------------------------------
// FlvFetchOffsetsRequest
// -----------------------------------

/// sets of offset update send by SPU to clients
#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaOffsetUpdateRequest {
    pub offsets: Vec<ReplicaOffsetUpdate>,
}

#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaOffsetUpdate {
    pub replica: ReplicaKey,
    pub error_code: ErrorCode,
    /// beginning offset for replica
    pub start_offset: Offset,
    /// offset for last record
    pub leo: Offset,
    /// offset for last committed record          
    pub hw: Offset,
}

impl Request for ReplicaOffsetUpdateRequest {
    const API_KEY: u16 = SpuClientApiKey::ReplicaOffsetUpdate as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = ReplicaOffsetUpdateResponse;
}

// no content, this is one way request
#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaOffsetUpdateResponse {}
