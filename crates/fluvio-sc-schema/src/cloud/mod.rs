use std::fmt::Debug;

use anyhow::Result;

use fluvio_protocol::{Decoder, Encoder, Version};
use fluvio_protocol::api::Request;
use cloud_sc_extra::CloudRemoteClusterSpec;
use cloud_sc_extra::CloudStatus;
use cloud_sc_extra::remote::CloudRemoteClusterRequest;

use crate::{AdminPublicApiKey, TryEncodableFrom};
use crate::objects::{COMMON_VERSION, TypeBuffer};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectCloudRequest(TypeBuffer);

impl Request for ObjectCloudRequest {
    const API_KEY: u16 = AdminPublicApiKey::Cloud as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = CloudStatus;
}

impl<S> TryEncodableFrom<CloudRemoteClusterRequest<S>> for ObjectCloudRequest
where
    CloudRemoteClusterRequest<S>: Encoder + Decoder + Debug,
    S: CloudRemoteClusterSpec + fluvio_controlplane_metadata::core::Spec,
{
    fn try_encode_from(input: CloudRemoteClusterRequest<S>, version: Version) -> Result<Self> {
        Ok(Self(TypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<CloudRemoteClusterRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}
