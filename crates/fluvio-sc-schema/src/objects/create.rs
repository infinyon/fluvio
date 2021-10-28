#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;

use crate::{AdminPublicApiKey, AdminSpec, CreateDecoder, Status};

use super::{ObjectApiEnum, ObjectApiDecode};

ObjectApiEnum!(CreateRequest);

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: AdminSpec> {
    pub name: String,
    pub dry_run: bool,
    pub spec: S,
}

impl Request<CreateDecoder> for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = Status;

    ObjectApiDecode!(CreateRequest, CreateDecoder);
}

/// Used for compatibility with older versions of the API
pub enum CreateType {
    Topic = 0,
    CustomSPU = 1,
    SPG = 2,
    ManagedConnector = 3,
    SmartModule = 4,
    TABLE = 5,
    SmartStream = 6,
}
