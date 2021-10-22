#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use fluvio_controlplane_metadata::store::Epoch;
use fluvio_controlplane_metadata::message::Message;


use crate::{AdminPublicApiKey,AdminRequest,AdminSpec};

use super::Metadata;

/// marker trait for List
pub trait WatchSpec: AdminSpec {
    /// convert to list request with filters
    #[allow(clippy::wrong_self_convention)]
    fn into_list_request(epoch: Epoch) -> WatchRequest<Self>;
}

/// Watch resources
/// Argument epoch is not being used, it is always 0
#[derive(Debug,Encoder,Default, Decoder )]
pub struct WatchRequest<S: AdminSpec> {
    epoch: Epoch,
    data: PhantomData<S>
}


impl <S>Request for WatchRequest<S> where S: AdminSpec
{
    const API_KEY: u16 = AdminPublicApiKey::Watch as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = WatchResponse<S>;
}

impl <S>AdminRequest for WatchRequest<S> where S: AdminSpec 
{}

#[derive(Debug)]
pub struct WatchResponse<S: AdminSpec>  
{
    type_string: String,
    inner: MetadataUpdate<S>
}

impl <S>Default for WatchResponse<S>
    where S: AdminSpec
{
    fn default() -> Self {
        Self {
            type_string: S::LABEL,
            inner: MetadataUpdate::default()
        }
    }
}


/// updates on metadata
#[derive(Encoder, Decoder, Default, Clone, Debug)]
pub struct MetadataUpdate<S: AdminSpec>
{
    pub epoch: Epoch,
    pub changes: Vec<Message<Metadata<S>>>,
    pub all: Vec<Metadata<S>>,
}

impl<S> MetadataUpdate<S> where S: AdminSpec
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
