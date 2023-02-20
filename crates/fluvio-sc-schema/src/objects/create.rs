#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::io::Cursor;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::api::Request;
<<<<<<< HEAD
use fluvio_protocol::Version;
=======
use fluvio_protocol::core::ByteBuf;
>>>>>>> 8823db0a (wip)

use crate::{AdminPublicApiKey, CreatableAdminSpec, Status, TryEncodableFrom};

/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
}

#[derive(Debug)]
pub struct CreateRequest<R> {
    common: CommonCreateRequest,
    spec: R,
}

impl<S> CreateRequest<S> {
    pub fn new(common: CommonCreateRequest, spec: S) -> Self {
        Self { common, spec }
    }

    /// deconstruct
    pub fn parts(self) -> (CommonCreateRequest, S) {
        (self.common, self.spec)
    }
}

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = Status;
}

#[derive(Debug, Default, Encoder, Decoder)]
pub struct ObjectApiCreateRequest {
    common: CommonCreateRequest,
    encoded_spec: CreateTypeBuffer,
}

impl<S> TryEncodableFrom<CreateRequest<S>> for ObjectApiCreateRequest
where
    S: CreatableAdminSpec,
{
    fn try_encode_from(request: CreateRequest<S>, version: Version) -> Result<Self> {
        let CreateRequest { common, spec } = request;
        let encoded_spec = CreateTypeBuffer::encode(spec, version)?;
        Ok(Self {
            common,
            encoded_spec,
        })
    }

    fn downcast(&self) -> Result<Option<CreateRequest<S>>> {
        Ok(self.encoded_spec.downcast()?.map(|spec| CreateRequest {
            common: self.common.clone(),
            spec,
        }))
    }
}

/// special type buffer for create request
/// unlike other request, this uses int for legacy reason
#[derive(Encoder, Decoder, Default, Debug)]
pub struct CreateTypeBuffer {
    pub ty: u8,
    pub buf: ByteBuf,
}

impl CreateTypeBuffer {
    fn encode<S: CreatableAdminSpec>(spec: S, version: Version) -> Result<Self> {
        let mut buf = vec![];
        spec.encode(&mut buf, version)?;
        Ok(Self {
            ty: S::CREATE_TYPE,
            buf: ByteBuf::from(buf),
        })
    }

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
