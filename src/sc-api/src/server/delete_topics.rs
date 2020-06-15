//!
//! # Delete Topics
//!
//! Public API to request the SC to delete one or more topics.
//!
//!

use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use super::FlvResponseMessage;
use super::ScServerApiKey;

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteTopicsRequest {
    /// Each topic in the request.
    pub topics: Vec<String>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteTopicsResponse {
    /// A response message for each topic request
    pub results: Vec<FlvResponseMessage>,
}

impl Request for FlvDeleteTopicsRequest {
    const API_KEY: u16 = ScServerApiKey::FlvDeleteTopics as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvDeleteTopicsResponse;
}
