#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::io::Cursor;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
<<<<<<< HEAD
use fluvio_protocol::Version;
=======
use fluvio_protocol::core::ByteBuf;
>>>>>>> 8823db0a (wip)


use crate::{AdminPublicApiKey, CreatableAdminSpec, Status};


/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
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
    pub req: ObjectWrapper,
}

impl ObjectApiCreateRequest {

    /// encode admin spec into a request
    pub fn encode<S: CreatableAdminSpec>(common: CommonCreateRequest, spec: S) -> Result<Self> {

        let mut buf = vec![];
        spec.encode(&mut buf, 0)?;
       Ok(Self {
            common,
            req: ObjectWrapper {
                ty: S::CREATE_TYPE,
                buf: ByteBuf::from(buf),
            },
        })
    }
}



#[derive(Encoder, Decoder, Default, Debug)]
pub struct ObjectWrapper {
    pub ty: u8,
    pub buf: ByteBuf
}

impl ObjectWrapper {

    // check if this object is kind of spec
    pub fn is_kind_of<S: CreatableAdminSpec>(&self) -> bool {
        self.ty == S::CREATE_TYPE
    }

    /// try to decode as spec
    /// if it is not kind of spec, return None
    pub fn downcast<S: CreatableAdminSpec>(&self) -> Result<Option<S>> {
        if self.is_kind_of::<S>() {
            let mut buf = Cursor::new(self.buf.as_ref());
            Ok(Some(S::decode_from(&mut buf, 0)?))
        } else {
            Ok(None)
        }
    }
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
