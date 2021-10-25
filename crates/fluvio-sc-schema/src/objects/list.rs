#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use crate::{AdminObjectDecoder, AdminPublicApiKey, AdminSpec,ObjectDecoder};
use super::{ObjectApiEnum,ObjectApiDecode};

ObjectApiEnum!(ListRequest);
ObjectApiEnum!(ListResponse);

//pub type ObjListRequest = ObjectRequest<ObjectDecoder, ObjectApiListRequest>;
//pub type ObjListResponse = ObjectResponse<ObjectDecoder, ObjectApiListResponse>;

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

impl Request<ObjectDecoder> for ObjectApiListRequest
{
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ObjectApiListResponse;

    ObjectApiDecode!(ListRequest,ObjectDecoder);
}


#[derive(Debug, Default, Encoder, Decoder)]
pub struct ListResponse<S: AdminSpec>(Vec<S::ListType>);

impl<S> ListResponse<S>
where
    S: AdminSpec,
{
    pub fn new(list: Vec<S::ListType>) -> Self {
        Self(list)
    }
}
