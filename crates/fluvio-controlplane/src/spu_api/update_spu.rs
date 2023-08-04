#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

pub type UpdateSpuRequest = ControlPlaneRequest<SpuSpec>;

impl Request for UpdateSpuRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSpu as u16;
    type Response = UpdateSpuResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSpuResponse {}
