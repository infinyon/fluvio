#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
<<<<<<< HEAD
use fluvio_protocol::Version;
=======
use fluvio_protocol::core::ByteBuf;
>>>>>>> 8823db0a (wip)


use crate::{AdminPublicApiKey, CreatableAdminSpec, Status};

#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateRequest<S: CreatableAdminSpec> {
    pub request: S,
}

/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectApiCreateRequest {
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

impl ObjectApiCreateRequest {

    /// encode admin spec into a request
    pub fn encode<S: CreatableAdminSpec>(name: String, dry_run: bool,spec: S) -> Result<Self> {

        let mut dest = vec![];
        spec.encode(&mut dest, 0)?;
        Self {
            name,
            dry_run: false,
            timeout: None,
            spec: ObjectWrapper {
                ty: S::CREATE_TYPE,
                spec: request.encode(),
            },
        }
    }
}



#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectWrapper {
    pub ty: u8,
    pub spec: Vec<u128>,
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

/* 
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
*/

//pub(crate) use CreateFrom;

use super::COMMON_VERSION;
