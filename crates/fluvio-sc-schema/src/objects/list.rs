use std::fmt::Debug;
use std::io::Cursor;

use anyhow::Result;

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_protocol::{Encoder, Decoder, ByteBuf};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, AdminSpec};
use super::{COMMON_VERSION, Metadata};

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
pub struct ObjectApiListRequest {
    pub name_filters: ListFilters,
    #[fluvio(min_version = 10)]
    pub summary: bool, // if true, only return summary
}

impl ObjectApiListRequest {
    pub fn new(name_filters: impl Into<ListFilters>, summary: bool) -> Self {
        Self {
            name_filters: name_filters.into(),
            summary,
        }
    }
}

impl Request for ObjectApiListRequest {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = ObjectApiListResponse;
}

/// Type encoded buffer, it uses type label to determine type
#[derive(Debug, Default, Encoder, Decoder)]
pub struct TypeBuffer {
    ty: String,
    buf: ByteBuf,
}

impl TypeBuffer {
    // check if this object is kind of spec
    pub fn is_kind_of<S: AdminSpec>(&self) -> bool {
        self.ty == S::LABEL
    }

    // downcast to specific spec type and return object
    // if doens't match to ty, return None
    pub fn downcast<S, O>(&self) -> Result<Option<O>>
    where
        S: AdminSpec,
        O: Encoder + Decoder + Debug,
    {
        if self.is_kind_of::<S>() {
            let mut buf = Cursor::new(self.buf.as_ref());
            Ok(Some(O::decode_from(&mut buf, 0)?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiListResponse(TypeBuffer);

impl ObjectApiListResponse {
    pub fn downcast<S>(&self) -> Result<Option<Vec<Metadata<S>>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        self.0.downcast::<S, Vec<Metadata<S>>>()
    }
}
