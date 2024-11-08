#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::Result;

use fluvio_controlplane_metadata::message::Message;
use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::api::Request;
use fluvio_controlplane_metadata::store::Epoch;

use crate::{AdminPublicApiKey, AdminSpec, TryEncodableFrom};
use crate::core::Spec;

use super::classic::{ClassicObjectApiEnum, ClassicDecoding};
use super::{Metadata, COMMON_VERSION, TypeBuffer};

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug, Encoder, Default, Decoder)]
pub struct WatchRequest<S: AdminSpec> {
    epoch: Epoch,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
    data: PhantomData<S>,
}

impl<S> WatchRequest<S>
where
    S: AdminSpec,
{
    pub fn summary() -> Self {
        Self {
            summary: true,
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Encoder)]
pub struct ObjectApiWatchRequest(TypeBuffer);

impl<S> TryEncodableFrom<WatchRequest<S>> for ObjectApiWatchRequest
where
    S: AdminSpec,
{
    fn try_encode_from(input: WatchRequest<S>, version: Version) -> Result<Self> {
        Ok(Self(TypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<WatchRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}

impl Request for ObjectApiWatchRequest {
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const MIN_API_VERSION: i16 = 15;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiWatchResponse;
}

#[derive(Debug, Default, Encoder)]
pub struct ObjectApiWatchResponse(TypeBuffer);

impl<S> TryEncodableFrom<WatchResponse<S>> for ObjectApiWatchResponse
where
    S: AdminSpec,
    S::Status: Encoder + Decoder,
{
    fn try_encode_from(input: WatchResponse<S>, version: Version) -> Result<Self> {
        Ok(Self(TypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<WatchResponse<S>>> {
        self.0.downcast::<S, _>()
    }
}

#[derive(Debug, Default, Encoder, Decoder, Clone)]
pub struct WatchResponse<S: AdminSpec>
where
    S::Status: Encoder + Decoder,
{
    inner: MetadataUpdate<S>,
}

impl<S> WatchResponse<S>
where
    S: AdminSpec,
    S::Status: Encoder + Decoder,
{
    pub fn new(inner: MetadataUpdate<S>) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> MetadataUpdate<S> {
        self.inner
    }
}

/// updates on metadata
#[derive(Encoder, Decoder, Default, Clone, Debug)]
pub struct MetadataUpdate<S>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder + Debug,
{
    pub epoch: Epoch,
    pub changes: Vec<Message<Metadata<S>>>,
    pub all: Vec<Metadata<S>>,
}

impl<S> MetadataUpdate<S>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder + Debug,
{
    pub fn with_changes(epoch: i64, changes: Vec<Message<Metadata<S>>>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<Metadata<S>>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}

// for supporting classic, this should go away after we remove classic
ClassicObjectApiEnum!(WatchRequest);
ClassicObjectApiEnum!(WatchResponse);
ClassicDecoding!(WatchRequest);
ClassicDecoding!(WatchResponse);
