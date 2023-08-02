use fluvio_controlplane_metadata::message::{Message, Messages};
use fluvio_protocol::{Encoder, Decoder, api::Request};
use fluvio_stream_model::{store::MetadataStoreObject, core::MetadataItem};

use crate::{requests::ControlPlaneRequest, remote_cluster::RemoteClusterSpec};

use super::api::InternalSpuApi;

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct RemoteCluster {
    pub name: String,
    pub spec: RemoteClusterSpec,
}

pub type UpdateRemoteClusterRequest = ControlPlaneRequest<RemoteCluster>;

impl Request for UpdateRemoteClusterRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateRemoteCluster as u16;
    type Response = UpdateRemoteClusterResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateRemoteClusterResponse {}

pub type RemoteClusterMsg = Message<RemoteCluster>;
pub type RemoteClusterMsgs = Messages<RemoteCluster>;

impl<C> From<MetadataStoreObject<RemoteClusterSpec, C>> for RemoteCluster
where
    C: MetadataItem,
{
    fn from(mso: MetadataStoreObject<RemoteClusterSpec, C>) -> Self {
        let name = mso.key_owned();
        let spec = mso.spec;
        Self { name, spec }
    }
}
