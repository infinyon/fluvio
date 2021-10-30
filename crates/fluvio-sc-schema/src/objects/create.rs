#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use crate::{AdminPublicApiKey, AdminSpec, Status};

use super::{CreateApiEnum};

CreateApiEnum!(CreateRequest);

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: AdminSpec> {
    pub name: String,
    pub dry_run: bool,
    pub spec: S,
}

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = Status;
}
