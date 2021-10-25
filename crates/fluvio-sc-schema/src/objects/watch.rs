#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;

use crate::{AdminPublicApiKey, AdminRequest, AdminSpec};
use crate::core::Spec;

use super::{Metadata,ObjectApiEnum};

ObjectApiEnum!(WatchRequest);
ObjectApiEnum!(WatchResponse);

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug, Encoder, Default, Decoder)]
pub struct WatchRequest<S: AdminSpec> {
    epoch: Epoch,
    data: PhantomData<S>,
}

impl Request for ObjectApiWatchRequest
{
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ObjectApiWatchResponse;
}



#[derive(Debug, Default, Encoder, Decoder)]
pub struct WatchResponse<S: AdminSpec> {
    inner: S::WatchResponseType,
}

/// updates on metadata
#[derive(Encoder, Decoder, Default, Clone, Debug)]
pub struct MetadataUpdate<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
{
    pub epoch: Epoch,
    pub changes: Vec<Message<Metadata<S>>>,
    pub all: Vec<Metadata<S>>,
}

impl<S> MetadataUpdate<S>
where
    S: Spec + Debug + Encoder + Decoder,
    S::Status: Debug + Encoder + Decoder,
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
