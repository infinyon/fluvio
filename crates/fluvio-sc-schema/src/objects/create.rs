use std::fmt::Debug;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::api::Request;

use crate::{AdminPublicApiKey, CreatableAdminSpec, Status, TryEncodableFrom};

use super::{COMMON_VERSION};

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
    pub request: S,
}

impl<S> CreateRequest<S> {
    pub fn new(common: CommonCreateRequest, request: S) -> Self {
        Self { common, request }
    }

    /// deconstruct
    pub fn parts(self) -> (CommonCreateRequest, S) {
        (self.common, self.request)
    }
}

#[derive(Debug, Default, Encoder)]
pub struct ObjectApiCreateRequest(CreateTypeBuffer); // replace with CreateTypeBuffer with TypeBuffer after

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
        Ok(Self(CreateTypeBuffer::encode(input, version)?))
    }

    fn downcast(&self) -> Result<Option<CreateRequest<S>>> {
        self.0.downcast::<S, _>()
    }
}

use classic::*;

// backward compatibility with classic protocol. this should go away once we deprecate classic
mod classic {

    use std::io::{Error as IoError, ErrorKind, Cursor};
    use std::fmt::{Debug};

    use anyhow::Result;

    use fluvio_controlplane_metadata::core::Spec;
    use fluvio_protocol::{Decoder, ByteBuf, Version, Encoder};

    use crate::CreatableAdminSpec;
    use crate::objects::classic::{ClassicCreatableAdminSpec, ClassicObjectApiCreateRequest};
    use crate::objects::{
        classic::{ClassicObjectCreateRequest},
        COMMON_VERSION, DYN_OBJ,
    };

    use super::{ObjectApiCreateRequest, CreateRequest};

    // This sections for compatibility with classic protocol, should go away once we deprecate classic

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

    /// This is same as TypeBuffer, but need to have for create because
    /// classic protocol treated differently.  Once classic protocol is deprecated, we can remove this
    #[derive(Debug, Default)]
    pub(crate) struct CreateTypeBuffer {
        ty: String,
        buf: ByteBuf,
    }

    impl CreateTypeBuffer {
        // since this is create, we can specialize it
        pub(crate) fn encode<S>(input: CreateRequest<S>, version: Version) -> Result<Self>
        where
            S: CreatableAdminSpec,
        {
            let ty = S::LABEL.to_owned();
            let mut buf = vec![];
            if version >= DYN_OBJ {
                input.encode(&mut buf, version)?;
            } else {
                // for classical, we use old way
                let parts = input.parts();
                let request = <S as ClassicCreatableAdminSpec>::try_classic_convert(parts.1)?;
                let create_api_request = ClassicObjectApiCreateRequest {
                    common: parts.0,
                    request,
                };
                create_api_request.encode(&mut buf, version)?;
            }
            Ok(Self {
                ty,
                buf: ByteBuf::from(buf),
            })
        }

        // check if this object is kind of spec
        pub fn is_kind_of<S: Spec>(&self) -> bool {
            self.ty == S::LABEL
        }

        // downcast to specific spec type and return object
        // if doens't match to ty, return None
        pub fn downcast<S, O>(&self) -> Result<Option<O>>
        where
            S: Spec,
            O: Decoder + Debug,
        {
            if self.is_kind_of::<S>() {
                println!("is kind of: {:#?}", S::LABEL);
                let mut buf = Cursor::new(self.buf.as_ref());
                Ok(Some(O::decode_from(&mut buf, COMMON_VERSION)?))
            } else {
                println!("not kind of: {:#?}", S::LABEL);
                Ok(None)
            }
        }

        pub(crate) fn set_buf(&mut self, ty: String, buf: ByteBuf) {
            self.buf = buf;
            self.ty = ty;
        }
    }

    impl Encoder for CreateTypeBuffer {
        fn write_size(&self, version: Version) -> usize {
            if version >= DYN_OBJ {
                self.ty.write_size(version) + (0 as u32).write_size(version) + self.buf.len()
            } else {
                self.buf.len()
            }
        }

        fn encode<T>(&self, dest: &mut T, version: Version) -> std::result::Result<(), IoError>
        where
            T: fluvio_protocol::bytes::BufMut,
        {
            if version >= DYN_OBJ {
                self.ty.encode(dest, version)?;
                let len: u32 = self.buf.len() as u32;
                len.encode(dest, version)?; // write len
                println!("encoding using new with len: {:#?}", len);
            } else {
                println!("encoding using old with len: {}", self.buf.len());
            }
            dest.put(self.buf.as_ref());

            Ok(())
        }
    }

    // this is always using new protocol, classical decoding is done before by caller

    impl Decoder for CreateTypeBuffer {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> std::result::Result<(), IoError>
        where
            T: fluvio_protocol::bytes::Buf,
        {
            println!("decoding tybuffer using new protocol");
            self.ty.decode(src, version)?;
            tracing::trace!(ty = self.ty, "decoded type");
            println!("decoded type: {:#?}", self.ty);

            let mut len: u32 = 0;
            len.decode(src, version)?;
            tracing::trace!(len, "decoded len");
            println!("copy bytes: {:#?}", len);
            if src.remaining() < len as usize {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "not enough bytes, need: {}, remaining: {}",
                        len,
                        src.remaining()
                    ),
                ));
            }
            self.buf = src.copy_to_bytes(len as usize).into();

            Ok(())
        }
    }
}
