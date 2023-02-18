use std::fmt::Debug;

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

pub type ObjectApiListRequest = ListRequest;

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListRequest {
    pub name_filters: ListFilters,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
}

impl ListRequest {
    pub fn new(name_filters: impl Into<ListFilters>, summary: bool) -> Self {
        Self {
            name_filters: name_filters.into(),
            summary,
        }
    }
}

impl Request for ListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiListResponse;
}

pub type ObjectApiListResponse = ListResponse;

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListResponse(TypeBuffer);

impl ListResponse {

    pub fn encode<S>(input: Vec<Metadata<S>>) -> Result<Self> 
    where
    S: AdminSpec,
    S::Status: Encoder + Decoder + Debug,
    {

        Ok(Self(TypeBuffer::encode::<S,_>(input)?))
    }

    pub fn downcast<S>(&self) -> Result<Option<Vec<Metadata<S>>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        self.0.downcast::<S, _>()
    }
}
