use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Display};
use std::io::{Error as IoError, Cursor};
use std::io::ErrorKind;

use anyhow::Result;

use fluvio_protocol::{Encoder, Decoder, ByteBuf, Version};

use fluvio_controlplane_metadata::store::MetadataStoreObject;
use fluvio_controlplane_metadata::core::{MetadataContext, MetadataItem};

use crate::AdminSpec;
use crate::core::Spec;
use crate::objects::ObjectApiCreateRequest;
use crate::objects::classic::ClassicObjectCreateRequest;

use super::{COMMON_VERSION, DYN_OBJ};

#[derive(Encoder, Decoder, Default, Clone, Debug)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Metadata<S>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder,
{
    pub name: String,
    pub spec: S,
    pub status: S::Status,
}

impl<S, C> From<MetadataStoreObject<S, C>> for Metadata<S>
where
    S: Spec + Encoder + Decoder,
    S::IndexKey: ToString,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
{
    fn from(meta: MetadataStoreObject<S, C>) -> Self {
        Self {
            name: meta.key.to_string(),
            spec: meta.spec,
            status: meta.status,
        }
    }
}

impl<S> Metadata<S>
where
    S: AdminSpec + Encoder + Decoder,
    S::Status: Encoder + Decoder,
{
    pub fn summary(self) -> Self {
        Self {
            name: self.name,
            spec: self.spec.summary(),
            status: self.status,
        }
    }
}

impl<S, C> TryFrom<Metadata<S>> for MetadataStoreObject<S, C>
where
    S: Spec + Encoder + Decoder,
    S::Status: Encoder + Decoder,
    C: MetadataItem,
    <S as Spec>::IndexKey: TryFrom<String>,
    <<S as Spec>::IndexKey as TryFrom<String>>::Error: Display,
{
    type Error = IoError;

    fn try_from(value: Metadata<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            spec: value.spec,
            status: value.status,
            key: value.name.try_into().map_err(|err| {
                IoError::new(ErrorKind::InvalidData, format!("problem converting: {err}"))
            })?,
            ctx: MetadataContext::default(),
        })
    }
}

/// Type encoded buffer, it uses type label to determine type
#[derive(Debug, Default)]
pub struct TypeBuffer {
    ty: String,
    buf: ByteBuf,
}

impl TypeBuffer {
    // encode admin spec into a request
    pub fn encode<S, I>(input: I, version: Version) -> Result<Self>
    where
        S: Spec,
        I: Encoder,
    {
        let ty = S::LABEL.to_owned();
        let mut buf = vec![];
        input.encode(&mut buf, version)?;
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

impl Encoder for TypeBuffer {
    fn write_size(&self, version: Version) -> usize {
        self.ty.write_size(version)
            + self.buf.len()
            + (if version >= DYN_OBJ {
                let u32 = 0;
                u32.write_size(version)
            } else {
                0
            })
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> std::result::Result<(), IoError>
    where
        T: fluvio_protocol::bytes::BufMut,
    {
        self.ty.encode(dest, version)?;
        if version >= DYN_OBJ {
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

impl Decoder for TypeBuffer {
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

/// This is same as TypeBuffer but
#[derive(Debug, Default)]
pub struct CreateTypeBuffer {
    ty: String,
    buf: ByteBuf,
}

impl CreateTypeBuffer {
    // encode admin spec into a request
    pub fn encode<S, I>(input: I, version: Version) -> Result<Self>
    where
        S: Spec,
        I: Encoder,
    {
        let ty = S::LABEL.to_owned();
        let mut buf = vec![];
        input.encode(&mut buf, version)?;
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
            // for classic, we use int and buffer
            (0 as u8).write_size(version) + self.buf.len()
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
            // encode string type as integer for classic protocol
            let int_ty = ClassicObjectCreateRequest::convert_type_string_to_int(&self.ty)?;
            int_ty.encode(dest, version)?;
            println!("encoding using old with len: {}", self.buf.len());
        }
        dest.put(self.buf.as_ref());

        Ok(())
    }
}

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