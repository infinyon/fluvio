//!
//! # Delete Custon SPUs
//!
//! Public API to request the SC to delete one or more custom spus.
//!
//!
use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use crate::FlvResponseMessage;
use crate::ScApiKey;
use crate::common::flv_spus::FlvCustomSpu;

// -----------------------------------
// FlvDeleteCustomSpusRequest
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteCustomSpusRequest {
    /// Each spu name or id to be deleted.
    pub custom_spus: Vec<FlvCustomSpu>,
}

// -----------------------------------
// FlvDeleteTopicsResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteCustomSpusResponse {
    /// A response message for each delete request
    pub results: Vec<FlvResponseMessage>,
}

// -----------------------------------
// Implementation - FlvDeleteTopicsRequest
// -----------------------------------

impl Request for FlvDeleteCustomSpusRequest {
    const API_KEY: u16 = ScApiKey::FlvDeleteCustomSpus as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvDeleteCustomSpusResponse;
}
