#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec};
use super::{ObjectApiEnum, COMMON_VERSION};

ObjectApiEnum!(ListRequest);
ObjectApiEnum!(ListResponse);

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListRequest<S: AdminSpec> {
    pub name_filters: Vec<S::ListFilter>,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
}

impl<S> ListRequest<S>
where
    S: AdminSpec,
{
    pub fn new(name_filters: Vec<S::ListFilter>, summary: bool) -> Self {
        Self {
            name_filters,
            summary,
        }
    }
}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
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
