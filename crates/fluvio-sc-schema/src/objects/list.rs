#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::marker::PhantomData;

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec};
use super::{ObjectApiEnum, COMMON_VERSION, Metadata};

ObjectApiEnum!(ListRequest);
ObjectApiEnum!(ListResponse);

/// Filter for List
#[derive(Debug, Encoder, Decoder, Default)]
pub struct ListFilter {
    pub name: String,
}

impl From<String> for ListFilter {
    fn from(name: String) -> Self {
        Self { name }
    }
}

#[derive(Debug, Encoder, Decoder, Default)]
pub struct ListFilters {
    filters: Vec<ListFilter>,
}

impl From<Vec<ListFilter>> for ListFilters {
    fn from(filters: Vec<ListFilter>) -> Self {
        Self { filters }
    }
}

impl From<ListFilters> for Vec<ListFilter> {
    fn from(filter: ListFilters) -> Self {
        filter.filters
    }
}

impl KeyFilter<str> for ListFilters {
    fn filter(&self, value: &str) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        self.filters
            .iter()
            .filter(|key| key.name.filter(value))
            .count()
            > 0
    }
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListRequest<S: AdminSpec> {
    pub name_filters: ListFilters,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
    data: PhantomData<S>, // satisfy generic
}

impl<S> ListRequest<S>
where
    S: AdminSpec,
{
    pub fn new(name_filters: impl Into<ListFilters>, summary: bool) -> Self {
        Self {
            name_filters: name_filters.into(),
            summary,
            data: PhantomData,
        }
    }
}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiListResponse;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListResponse<S: AdminSpec>
where
    S::Status: Encoder + Decoder + Debug,
{
    inner: Vec<Metadata<S>>,
}

impl<S> ListResponse<S>
where
    S: AdminSpec,
    S::Status: Encoder + Decoder + Debug,
{
    pub fn new(inner: Vec<Metadata<S>>) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> Vec<Metadata<S>> {
        self.inner
    }
}
