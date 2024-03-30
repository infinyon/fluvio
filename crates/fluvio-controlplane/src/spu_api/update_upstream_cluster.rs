use std::fmt;

use fluvio_controlplane_metadata::message::{Message, Messages};
use fluvio_controlplane_metadata::upstream_cluster::UpstreamClusterSpec;
use fluvio_protocol::{Encoder, Decoder, api::Request};
use fluvio_stream_model::core::MetadataItem;
use fluvio_stream_model::store::MetadataStoreObject;

use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

#[derive(Decoder, Encoder, Eq, PartialEq, Clone, Default)]
pub struct UpstreamCluster {
    pub name: String,
    pub spec: UpstreamClusterSpec,
}

impl fmt::Debug for UpstreamCluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Upstream {} from: {} to: {}",
            self.name, self.spec.source_id, self.spec.target.endpoint
        )
    }
}

pub type UpdateUpstreamClusterRequest = ControlPlaneRequest<UpstreamCluster>;

impl Request for UpdateUpstreamClusterRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateUpstreamCluster as u16;
    type Response = UpdateUpstreamClusterResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateUpstreamClusterResponse {}

pub type UpstreamClusterMsg = Message<UpstreamCluster>;
pub type UpstreamClusterMsgs = Messages<UpstreamCluster>;

impl<C> From<MetadataStoreObject<UpstreamClusterSpec, C>> for UpstreamCluster
where
    C: MetadataItem,
{
    fn from(mso: MetadataStoreObject<UpstreamClusterSpec, C>) -> Self {
        let name = mso.key_owned();
        let spec = mso.spec;
        Self { name, spec }
    }
}
