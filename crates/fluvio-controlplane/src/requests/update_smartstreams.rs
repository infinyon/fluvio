#![allow(clippy::assign_op_pattern)]

use dataplane::derive::Decoder;
use dataplane::derive::Encoder;
use dataplane::api::Request;
use fluvio_controlplane_metadata::smartstream::SmartStreamSpec;
use crate::InternalSpuApi;

use super::ControlPlaneRequest;

pub type UpdateSmartStreamRequest = ControlPlaneRequest<SmartStreamSpec>;

impl Request for UpdateSmartStreamRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSmartStream as u16;
    type Response = UpdateSmartStreamResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSmartStreamResponse {}
