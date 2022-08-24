#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_protocol::api::Request;
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
