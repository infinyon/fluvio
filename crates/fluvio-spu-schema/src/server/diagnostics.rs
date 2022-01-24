//!
//! API to fetch diagnostics.
use dataplane::api::Request;
use dataplane::core::{Encoder, Decoder};

use super::SpuServerApiKey;

#[derive(Decoder, Encoder, Default, Debug)]
pub struct DiagnosticsRequest {}

impl Request for DiagnosticsRequest {
    const API_KEY: u16 = SpuServerApiKey::Diagnostics as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = DiagnosticsResponse;
}

#[derive(Decoder, Encoder, Default, Debug, PartialEq)]
pub struct DiagnosticsResponse {
    //  topics: Vec<DiagnosticsTopic>,
}

pub struct Topic {
    pub name: String,
    //  pub partitions: Vec<DiagnosticsPartition>,
}
