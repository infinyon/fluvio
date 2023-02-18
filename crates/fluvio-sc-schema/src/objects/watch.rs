#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{Request};
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;

use crate::{AdminPublicApiKey, AdminSpec};
use crate::core::Spec;

use super::{Metadata, COMMON_VERSION, TypeBuffer};

pub type ObjectApiWatchRequest = WatchRequest;

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug, Encoder, Default, Decoder)]
pub struct WatchRequest {
    epoch: Epoch,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
}

impl WatchRequest {
    pub fn summary() -> Self {
        Self {
            summary: true,
            ..Default::default()
        }
    }
}

impl Request for WatchRequest {
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiWatchResponse;
}

pub type ObjectApiWatchResponse = WatchResponse;

#[derive(Debug, Default, Encoder, Decoder)]
pub struct WatchResponse(TypeBuffer);

impl WatchResponse 
{

    pub fn encode<S>(update: MetadataUpdate<S>) -> Result<Self> 
    where
    S: AdminSpec,
    S::Status: Encoder + Decoder + Debug,
    {

        Ok(Self(TypeBuffer::encode::<S,_>(update)?))
    }

    pub fn downcast<S>(&self) -> Result<Option<MetadataUpdate<S>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        self.0.downcast::<S, _>()
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
