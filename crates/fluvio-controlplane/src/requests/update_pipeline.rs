use dataplane::api::Request;
use dataplane::derive::Decoder;
use dataplane::derive::Encoder;

use crate::InternalSpuApi;

/// Changes to Spu specs
#[derive(Decoder, Encoder, Debug, Default)]
pub struct UpdatePipelineRequest {}

impl Request for UpdatePipelineRequest {
    const API_KEY: u16 = InternalSpuApi::UpdatePipeline as u16;
    type Response = UpdatePipelineResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdatePipelineResponse {}
