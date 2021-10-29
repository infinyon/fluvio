#![allow(clippy::assign_op_pattern)]

use dataplane::derive::Decoder;
use dataplane::derive::Encoder;
use dataplane::api::Request;
use fluvio_controlplane_metadata::message::{SmartModuleMsg};
use fluvio_controlplane_metadata::smartmodule::SmartModule;
use crate::InternalSpuApi;

/// Changes to Replica Specs
#[derive(Decoder, Encoder, Debug, Default)]
pub struct UpdateSmartModuleRequest {
    pub epoch: i64,
    pub changes: Vec<SmartModuleMsg>,
    pub all: Vec<SmartModule>,
}

impl Request for UpdateSmartModuleRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSmartModule as u16;
    type Response = UpdateSmartModuleResponse;
}

impl UpdateSmartModuleRequest {
    pub fn with_changes(epoch: i64, changes: Vec<SmartModuleMsg>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<SmartModule>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSmartModuleResponse {}
