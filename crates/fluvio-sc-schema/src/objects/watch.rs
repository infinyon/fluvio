#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::{Request};
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;

use crate::{AdminPublicApiKey, AdminSpec};
use crate::core::Spec;

use super::{Metadata, ObjectApiEnum};

ObjectApiEnum!(WatchRequest);
ObjectApiEnum!(WatchResponse);

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug, Encoder, Default, Decoder)]
pub struct WatchRequest<S: AdminSpec> {
    epoch: Epoch,
    data: PhantomData<S>,
}

impl Request for ObjectApiWatchRequest {
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = 6;
    type Response = ObjectApiWatchResponse;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct WatchResponse<S: AdminSpec>
where
    <S::WatchResponseType as Spec>::Status: Encoder + Decoder,
{
    inner: MetadataUpdate<S::WatchResponseType>,
}

impl<S> WatchResponse<S>
where
    S: AdminSpec,
    <S::WatchResponseType as Spec>::Status: Encoder + Decoder,
{
    pub fn new(inner: MetadataUpdate<S::WatchResponseType>) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> MetadataUpdate<S::WatchResponseType> {
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
