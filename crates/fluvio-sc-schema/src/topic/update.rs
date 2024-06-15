use std::fmt::Debug;

use anyhow::Result;

use fluvio_controlplane_metadata::topic::{
    TopicUpdateRequest, TopicUpdateSpec, TopicUpdateStatusResponse,
};
use fluvio_protocol::{Decoder, Encoder, Version};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, TryEncodableFrom};
use crate::objects::{COMMON_VERSION, TypeBuffer};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectTopicUpdateRequest(TypeBuffer);

impl Request for ObjectTopicUpdateRequest {
    const API_KEY: u16 = AdminPublicApiKey::TopicUpdate as u16;
    const MIN_API_VERSION: i16 = 15;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = TopicUpdateStatusResponse;
}

impl<S> TryEncodableFrom<TopicUpdateRequest<S>> for ObjectTopicUpdateRequest
where
    TopicUpdateRequest<S>: Encoder + Decoder + Debug,
    S: TopicUpdateSpec + fluvio_controlplane_metadata::core::Spec,
{
    fn try_encode_from(input: TopicUpdateRequest<S>, version: Version) -> Result<Self> {
        Ok(Self(TypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<TopicUpdateRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}
