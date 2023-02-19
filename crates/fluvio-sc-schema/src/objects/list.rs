use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::Result;

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec};
use super::{COMMON_VERSION, Metadata, TypeBuffer};

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

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiListRequest(TypeBuffer);

impl ObjectApiListRequest {

    pub fn encode<S>(input: ListRequest<S>) -> Result<Self>
    where
        S: AdminSpec,
    {
        Ok(Self(TypeBuffer::encode::<S, _>(input)?))
    }

    pub fn downcast<S>(&self) -> Result<Option<ListRequest<S>>>
    where
        S: AdminSpec
    {
        self.0.downcast::<S, _>()
    }

}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiListResponse;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiListResponse(TypeBuffer);

impl ObjectApiListResponse {
    pub fn encode<S>(input: ListResponse<S>) -> Result<Self>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        Ok(Self(TypeBuffer::encode::<S, _>(input)?))
    }

    pub fn downcast<S>(&self) -> Result<Option<ListResponse<S>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        self.0.downcast::<S, _>()
    }
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
