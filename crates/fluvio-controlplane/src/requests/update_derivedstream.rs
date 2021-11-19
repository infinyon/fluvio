#![allow(clippy::assign_op_pattern)]

use dataplane::derive::Decoder;
use dataplane::derive::Encoder;
use dataplane::api::Request;
use fluvio_controlplane_metadata::message::DerivedStreamControlData;
use crate::InternalSpuApi;

use super::ControlPlaneRequest;

pub type UpdateDerivedStreamRequest = ControlPlaneRequest<DerivedStreamControlData>;

impl Request for UpdateDerivedStreamRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateDerivedStream as u16;
    type Response = UpdateDerivedStreamResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateDerivedStreamResponse {}
