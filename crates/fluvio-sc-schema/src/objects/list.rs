use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::Result;

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec, TryEncodableFrom};
use super::{COMMON_VERSION, Metadata, TypeBuffer};
use super::classic::{ClassicObjectApiEnum, ClassicDecoding};

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

impl From<&str> for ListFilters {
    fn from(name: &str) -> Self {
        Self {
            filters: vec![ListFilter {
                name: name.to_owned(),
            }],
        }
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
pub struct ListRequest<S> {
    pub name_filters: ListFilters,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
    #[fluvio(min_version = 13)]
    pub system: bool, // if true, only return system specs
    data: PhantomData<S>, // satisfy generic
}

impl<S> ListRequest<S> {
    pub fn new(name_filters: impl Into<ListFilters>, summary: bool) -> Self {
        Self {
            name_filters: name_filters.into(),
            summary,
            system: false,
            data: PhantomData,
        }
    }

    pub fn system(mut self, system: bool) -> Self {
        self.system = system;
        self
    }
}

#[derive(Debug, Default, Encoder)]
pub struct ObjectApiListRequest(TypeBuffer);

impl<S> TryEncodableFrom<ListRequest<S>> for ObjectApiListRequest
where
    ListRequest<S>: Encoder + Decoder + Debug,
    S: AdminSpec,
{
    fn try_encode_from(input: ListRequest<S>, version: Version) -> Result<Self> {
        Ok(Self(TypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<ListRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const MIN_API_VERSION: i16 = 15;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiListResponse;
}

#[derive(Debug, Default, Encoder)]
pub struct ObjectApiListResponse(TypeBuffer);

impl<S> TryEncodableFrom<ListResponse<S>> for ObjectApiListResponse
where
    S: AdminSpec,
    S::Status: Encoder + Decoder + Debug,
{
    fn try_encode_from(input: ListResponse<S>, version: Version) -> Result<ObjectApiListResponse> {
        Ok(ObjectApiListResponse(TypeBuffer::encode::<S, _>(
            input, version,
        )?))
    }

    fn downcast(&self) -> Result<Option<ListResponse<S>>> {
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

// for supporting classic, this should go away after we remove classic
ClassicObjectApiEnum!(ListRequest);
ClassicObjectApiEnum!(ListResponse);
ClassicDecoding!(ListRequest);
ClassicDecoding!(ListResponse);
