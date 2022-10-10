#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec};
use super::{ObjectApiEnum};

ObjectApiEnum!(ListRequest);
ObjectApiEnum!(ListResponse);

pub const MIN_API_WITH_FILTER: u16 = 10;

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListRequest<S: AdminSpec> {
    pub name_filters: Vec<S::ListFilter>,
}

impl<S> ListRequest<S>
where
    S: AdminSpec,
{
    pub fn new(name_filters: Vec<S::ListFilter>) -> Self {
        Self { name_filters }
    }
}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 10;
    type Response = ObjectApiListResponse;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListResponse<S: AdminSpec> {
    inner: Vec<S::ListType>,
}

impl<S> ListResponse<S>
where
    S: AdminSpec,
{
    pub fn new(inner: Vec<S::ListType>) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> Vec<S::ListType> {
        self.inner
    }
}
