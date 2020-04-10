//!
//! # Fetch Local SPU
//!
//! Public API to fetch local SPU metadata from the SPU
//!
use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

use crate::SpuApiKey;
use crate::errors::FlvErrorCode;

// -----------------------------------
// FlvFetchLocalSpuRequest
// -----------------------------------

#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchLocalSpuRequest {}

// -----------------------------------
// FlvFetchLocalSpuResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchLocalSpuResponse {
    /// Spu lookup error code, None for no error
    pub error_code: FlvErrorCode,

    /// Spu name. A unique key in Key/Value stores such as Kubernetes.
    pub name: String,

    /// Spu id. Managed Spu ids start from 0. Custom SPU ids start from 5000.
    pub id: i32,


    /// Public endpoint server interface.
    pub public_ep: String,

    /// Private endpoint server interface.
    pub private_ep: String,

    /// Rack label, optional parameter used by replica assignment algorithm.
    pub rack: Option<String>,
}

// -----------------------------------
// Implementation - FlvFetchLocalSpuRequest
// -----------------------------------

impl Request for FlvFetchLocalSpuRequest {
    const API_KEY: u16 = SpuApiKey::FlvFetchLocalSpu as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = FlvFetchLocalSpuResponse;
}
