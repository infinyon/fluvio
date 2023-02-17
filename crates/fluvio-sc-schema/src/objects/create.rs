#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use fluvio_protocol::bytes::{BufMut, Buf};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_protocol::Version;

use crate::topic::TopicSpec;
use crate::customspu::CustomSpuSpec;
use crate::smartmodule::SmartModuleSpec;
use crate::tableformat::TableFormatSpec;
use crate::spg::SpuGroupSpec;

use crate::{AdminPublicApiKey, CreatableAdminSpec, Status, AdminSpec};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: CreatableAdminSpec> {
    pub request: S,
}

/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
    pub spec: ObjectWrapper,
}

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = Status;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiCreateRequest {
    pub common: CommonCreateRequest,
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectWrapper {
    pub ty: u8,
    pub spec: ByteBuf,
}

/// Macro to convert create request
/// impl From<(CommonCreateRequest TopicSpec)> for ObjectApiCreateRequest {
/// fn from(req: (CommonCreateRequest TopicSpec)) -> Self {
///       ObjectApiCreateRequest {
///           common: req.0,
///           request: req.1
///       }
/// }
/// ObjectFrom!(WatchRequest, Topic);

macro_rules! CreateFrom {
    ($create:ty,$specTy:ident) => {
        impl From<(crate::objects::CommonCreateRequest, $create)>
            for crate::objects::ObjectApiCreateRequest
        {
            fn from(fr: (crate::objects::CommonCreateRequest, $create)) -> Self {
                crate::objects::ObjectApiCreateRequest {
                    common: fr.0,
                    request: crate::objects::ObjectCreateRequest::$specTy(fr.1),
                }
            }
        }
    };
}

pub(crate) use CreateFrom;

use super::COMMON_VERSION;
