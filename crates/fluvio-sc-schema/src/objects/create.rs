#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::api::Request;


use crate::objects::classic::ClassicObjectCreateRequest;
use crate::{AdminPublicApiKey, CreatableAdminSpec, Status, TryEncodableFrom};

#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct CommonCreateRequest {
    pub name: String,
    pub dry_run: bool,
    #[fluvio(min_version = 7)]
    pub timeout: Option<u32>, // timeout in milliseconds
}


/// Every create request must have this parameters
#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct CreateRequest<S> {
    pub common: CommonCreateRequest,
    pub request: S
}

impl<S> CreateRequest<S> {
    pub fn new(common: CommonCreateRequest, request: S) -> Self {
        Self {
           common,
           request
        }
    }

    /// deconstruct
    pub fn parts(self) -> (CommonCreateRequest, S) {
        (self.common, self.request)
    }
}



#[derive(Debug, Default, Encoder)]
pub struct ObjectApiCreateRequest(CreateTypeBuffer);

impl Request for ObjectApiCreateRequest {
    const API_KEY: u16 = AdminPublicApiKey::Create as u16;
    const MIN_API_VERSION: i16 = 9;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;
    type Response = Status;
}

impl<S> TryEncodableFrom<CreateRequest<S>> for ObjectApiCreateRequest
where
    CreateRequest<S>: Encoder + Decoder + Debug,
    S: CreatableAdminSpec,
{
    fn try_encode_from(input: CreateRequest<S>, version: Version) -> Result<Self> {
        Ok(Self(CreateTypeBuffer::encode::<S, _>(input, version)?))
    }

    fn downcast(&self) -> Result<Option<CreateRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}

//pub(crate) use CreateFrom;
use super::{COMMON_VERSION, CreateTypeBuffer};

// this is for compatibility with older version
impl Decoder for ObjectApiCreateRequest {
    fn decode<T>(
        &mut self,
        src: &mut T,
        version: fluvio_protocol::Version,
    ) -> Result<(), std::io::Error>
    where
        T: fluvio_protocol::bytes::Buf,
    {
        if version >= crate::objects::DYN_OBJ {
            println!("decoding new");
            self.0.decode(src, version)?;
        } else {
            println!("decoding classical");

            let classic_obj = ClassicObjectCreateRequest::decode_from(src, version)?;
            // reencode using new version
            self.0.set_buf(
                classic_obj.type_string().to_owned(),
                classic_obj.as_bytes(COMMON_VERSION)?.into(),
            );
        }
        Ok(())
    }
}
