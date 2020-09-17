use std::io::Error as IoError;

use log::trace;

use flv_future_aio::bytes::Bytes;
use flv_future_aio::bytes::BytesMut;
use flv_future_aio::fs::AsyncFileSlice;

use crate::api::Request;
use crate::api::RequestMessage;
use crate::api::ResponseMessage;
use crate::{Encoder, Version};

pub enum StoreValue {
    Bytes(Bytes),
    FileSlice(AsyncFileSlice),
}

impl std::fmt::Debug for StoreValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StoreValue::Bytes(bytes) => write!(f, "StoreValue:Bytes with len: {}", bytes.len()),
            StoreValue::FileSlice(slice) => write!(f, "StoreValue:FileSlice: {:#?}", slice),
        }
    }
}

pub trait FileWrite: Encoder {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        _data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        self.encode(src, version)
    }
}

impl<M> FileWrite for Vec<M>
where
    M: FileWrite,
{
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        let len: i32 = self.len() as i32;
        len.encode(src, version)?;
        for v in self {
            v.file_encode(src, data, version)?;
        }
        Ok(())
    }
}

/// This is same as encoding in the ResponseMessage but can encode async file slice
impl<P> FileWrite for ResponseMessage<P>
where
    P: FileWrite + Default,
{
    fn file_encode(
        &self,
        dest: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        let len = self.write_size(version) as i32;
        log::debug!(
            "encoding file write response: {} version: {}, len: {}",
            std::any::type_name::<P>(),
            version,
            len
        );
        len.encode(dest, version)?;

        trace!("encoding response correlation  id: {}", self.correlation_id);
        self.correlation_id.encode(dest, version)?;

        trace!("encoding response");
        self.response.file_encode(dest, data, version)?;
        Ok(())
    }
}

impl<R> FileWrite for RequestMessage<R>
where
    R: FileWrite + Default + Request,
{
    fn file_encode(
        &self,
        dest: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding response message");
        let len = self.write_size(version) as i32;
        trace!("file encoding response len: {}", len);
        len.encode(dest, version)?;

        trace!("file encoding header");
        self.header.encode(dest, version)?;

        trace!("encoding response");
        self.request.file_encode(dest, data, version)?;
        Ok(())
    }
}
