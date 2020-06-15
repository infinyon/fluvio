//!
//! # Fetch SPUs
//!
//! Public API to fetch SPU metadata from the SC
//!
use kf_protocol::api::Request;
use kf_protocol::api::FlvErrorCode;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

use crate::ScPublicApiKey;
use super::FlvRequestSpuType;
use super::FlvSpuType;
use super::FlvSpuResolution;
use super::FlvEndPointMetadata;

// -----------------------------------
// FlvFetchSpusRequest
// -----------------------------------

/// Fetch SPUs by type
#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchSpusRequest {
    /// SPU type All or Custom
    pub req_spu_type: FlvRequestSpuType,
}

impl Request for FlvFetchSpusRequest {
    const API_KEY: u16 = ScPublicApiKey::FlvFetchSpus as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvFetchSpusResponse;
}


// -----------------------------------
// FlvFetchSpusResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchSpusResponse {
    /// Each spu in the response.
    pub spus: Vec<FlvFetchSpuResponse>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchSpuResponse {
    /// The error code, None for no errors
    pub error_code: FlvErrorCode,

    /// The spu name
    pub name: String,

    /// Spu parameters, None if error
    pub spu: Option<FlvFetchSpu>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchSpu {
    /// Spu globally unique id.
    pub id: i32,

    /// Spu type: true for managed, false for custom.
    pub spu_type: FlvSpuType,

    /// Public endpoint server interface.
    pub public_ep: FlvEndPointMetadata,

    /// Private endpoint server interface.
    pub private_ep: FlvEndPointMetadata,

    /// Rack label, optional parameter used by replica assignment algorithm.
    pub rack: Option<String>,

    /// Status resolution
    pub resolution: FlvSpuResolution,
}
