#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use crate::{AdminPublicApiKey, AdminRequest, AdminSpec};

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListRequest<S: AdminSpec> {
    name_filters: Vec<S::ListFilter>,
}

impl<S> ListRequest<S>
where
    S: AdminSpec,
{
    pub fn new(name_filters: Vec<S::ListFilter>) -> Self {
        Self {
            name_filters,
        }
    }
}

impl<S> Request for ListRequest<S>
where
    S: AdminSpec,
{
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ListResponse<S>;
}

impl<S> AdminRequest for ListRequest<S> where S: AdminSpec {}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListResponse<S: AdminSpec>(Vec<S::ListType>);

impl <S> ListResponse<S>
 where S: AdminSpec
{
    pub fn new(list: Vec<S::ListType>) -> Self {
        Self(list)
    }
}


