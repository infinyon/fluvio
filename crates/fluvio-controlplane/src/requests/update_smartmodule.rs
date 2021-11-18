#![allow(clippy::assign_op_pattern)]

use dataplane::derive::Decoder;
use dataplane::derive::Encoder;
use dataplane::api::Request;

use fluvio_controlplane_metadata::smartmodule::SmartModule;

use crate::InternalSpuApi;
use super::ControlPlaneRequest;

pub type UpdateSmartModuleRequest = ControlPlaneRequest<SmartModule>;

impl Request for UpdateSmartModuleRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSmartModule as u16;
    type Response = UpdateSmartModuleResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSmartModuleResponse {}
