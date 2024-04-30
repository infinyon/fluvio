use fluvio_controlplane_metadata::{
    core::MetadataItem,
    message::{Message, Messages},
    mirror::MirrorSpec,
    store::MetadataStoreObject,
};
use fluvio_protocol::{Encoder, Decoder, api::Request};

use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Mirror {
    pub name: String,
    pub spec: MirrorSpec,
}

pub type UpdateMirrorRequest = ControlPlaneRequest<Mirror>;

impl Request for UpdateMirrorRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateMirror as u16;
    type Response = UpdateMirrorResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateMirrorResponse {}

pub type MirrorMsg = Message<Mirror>;
pub type MirrorMsgs = Messages<Mirror>;

impl<C> From<MetadataStoreObject<MirrorSpec, C>> for Mirror
where
    C: MetadataItem,
{
    fn from(mso: MetadataStoreObject<MirrorSpec, C>) -> Self {
        let name = mso.key;
        let spec = mso.spec;
        Self { name, spec }
    }
}
