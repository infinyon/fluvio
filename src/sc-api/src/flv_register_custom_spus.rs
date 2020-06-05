//!
//! # Create Custom SPU
//!
//! Public API to request the SC to create one or more custom spus
//!
//!
use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use crate::FlvResponseMessage;
use crate::ScApiKey;
use crate::ApiError;

use super::spu::FlvEndPointMetadata;

// -----------------------------------
// FlvRegisterCustomSpusRequest
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvRegisterCustomSpusRequest {
    /// A list of one or more custom spus to be created.
    pub custom_spus: Vec<FlvRegisterCustomSpuRequest>,
}

impl Request for FlvRegisterCustomSpusRequest {
    const API_KEY: u16 = ScApiKey::FlvRegisterCustomSpus as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvRegisterCustomSpusResponse;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvRegisterCustomSpuRequest {
    /// The id of the custom spu (globally unique id)
    pub id: i32,

    /// The name of the custom spu
    pub name: String,

    /// Server host and port number of the public server
    pub public_server: FlvEndPointMetadata,

    /// Server host and port number of the private server
    pub private_server: FlvEndPointMetadata,

    /// Rack name (optional)
    pub rack: Option<String>,
}

// -----------------------------------
// FlvRegisterCustomSpusResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvRegisterCustomSpusResponse {
    /// The custom spu creation result messages.
    pub results: Vec<FlvResponseMessage>,
}

impl FlvRegisterCustomSpusResponse {
    /// validate and extract a single response
    pub fn validate(self, name: &str) -> Result<(), ApiError> {
        if let Some(item) = self.results.into_iter().find(|m| m.name == name) {
            item.as_result()
        } else {
            Err(ApiError::NoResourceFounded(name.to_owned()))
        }
    }
}
