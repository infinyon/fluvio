#![allow(clippy::assign_op_pattern)]

use dataplane::api::Request;
use dataplane::derive::Decoder;
use dataplane::derive::Encoder;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::InternalSpuApi;

use super::ControlPlaneRequest;

pub type UpdateSpuRequest = ControlPlaneRequest<SpuSpec>;

impl Request for UpdateSpuRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSpu as u16;
    type Response = UpdateSpuResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSpuResponse {}
