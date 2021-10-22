#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;


use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;


use crate::{AdminPublicApiKey,AdminRequest,AdminSpec};


#[derive(Debug,Default,Encoder,Decoder)]
pub struct ListRequest<S: AdminSpec> {
    object_type: String,
    name_filters: Vec<S::ListFilter>
}



impl <S>Request for ListRequest<S> where S: AdminSpec
 {
    const API_KEY: u16 = AdminPublicApiKey::List as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = ListResponse<S>;
}

impl <S>AdminRequest for ListRequest<S> where S: AdminSpec
{}



#[derive(Debug,Default,Encoder,Decoder)]
pub struct ListResponse<S: AdminSpec> {
    pub object_type: String,
    objects: Vec<S::ListType>,
}

