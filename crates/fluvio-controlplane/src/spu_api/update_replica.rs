use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;

use crate::replica::Replica;
use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

pub type UpdateReplicaRequest = ControlPlaneRequest<Replica>;

impl Request for UpdateReplicaRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateReplica as u16;
    type Response = UpdateReplicaResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateReplicaResponse {}
