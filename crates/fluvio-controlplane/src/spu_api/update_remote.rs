use fluvio_controlplane_metadata::{
    message::{Message, Messages},
    remote::RemoteSpec,
};
use fluvio_stream_model::{store::MetadataStoreObject, core::MetadataItem};
use fluvio_protocol::{Encoder, Decoder, api::Request};

use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Remote {
    pub name: String,
    pub spec: RemoteSpec,
}

pub type UpdateRemoteRequest = ControlPlaneRequest<Remote>;

impl Request for UpdateRemoteRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateRemote as u16;
    type Response = UpdateRemoteResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateRemoteResponse {}

pub type RemoteMsg = Message<Remote>;
pub type RemoteMsgs = Messages<Remote>;

impl<C> From<MetadataStoreObject<RemoteSpec, C>> for Remote
where
    C: MetadataItem,
{
    fn from(mso: MetadataStoreObject<RemoteSpec, C>) -> Self {
        let name = mso.key_owned();
        let spec = mso.spec;
        Self { name, spec }
    }
}
