use std::fs::File;
use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Read;
use std::path::Path;

use tracing::debug;
use tracing::trace;

use bytes::{Buf, BufMut};
use crate::api::RequestHeader;
use crate::{Decoder, Encoder, Version};

use super::DefaultRequestMiddleWare;
use super::RequestMiddleWare;

#[derive(Debug, Default)]
pub struct ResponseMessage<P, M = DefaultRequestMiddleWare> {
    pub correlation_id: i32,
    pub middleware: M,
    pub response: P,
}

impl<P> ResponseMessage<P, DefaultRequestMiddleWare> {
    pub fn from_header(header: &RequestHeader, response: P) -> Self {
        Self::new(header.correlation_id(), response)
    }

    pub fn new(correlation_id: i32, response: P) -> Self {
        Self {
            correlation_id,
            middleware: DefaultRequestMiddleWare::default(),
            response,
        }
    }
}

impl<P, M> ResponseMessage<P, M> {
    pub fn from_header_with_mw(header: &RequestHeader, response: P, middleware: M) -> Self {
        Self {
            correlation_id: header.correlation_id(),
            middleware,
            response,
        }
    }
}

impl<P> ResponseMessage<P, DefaultRequestMiddleWare>
where
    P: Decoder,
{
    pub fn decode_from<T>(src: &mut T, version: Version) -> Result<Self, IoError>
    where
        T: Buf,
    {
        let mut correlation_id: i32 = 0;
        correlation_id.decode(src, version)?;
        trace!("decoded correlation id: {}", correlation_id);

        let response = P::decode_from(src, version)?;
        Ok(ResponseMessage {
            correlation_id,
            middleware: DefaultRequestMiddleWare::default(),
            response,
        })
    }

    pub fn decode_from_file<H: AsRef<Path>>(
        file_name: H,
        version: Version,
    ) -> Result<Self, IoError> {
        debug!("decoding from file: {:#?}", file_name.as_ref());
        let mut f = File::open(file_name)?;
        let mut buffer: [u8; 1000] = [0; 1000];

        f.read_exact(&mut buffer)?;
        let data = buffer.to_vec();

        let mut src = Cursor::new(&data);

        // ResponseMessage implementation of fluvio_protocol::storage::FileWrite trait first encodes the length
        // of the ResponseMessage
        let mut size: i32 = 0;
        size.decode(&mut src, version)?;
        trace!("decoded response size: {} bytes", size);

        if src.remaining() < size as usize {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "not enought for response",
            ));
        }
        Self::decode_from(&mut src, version)
    }
}

/*
impl<P, M> ResponseMessage<P, M>
where
    P: MiddlewareDecoder,
    M: RequestMiddleWare,
{
    pub fn decode_from<T>(src: &mut T, version: Version) -> Result<Self, IoError>
    where
        T: Buf,
    {
        let mut correlation_id: i32 = 0;
        correlation_id.decode(src, version)?;
        trace!(correlation_id, "using correlation id");

        let mut middleware = M::default();
        middleware.decode(src, version)?;
        let response = P::decode_from_with_middleware(src, &middleware, version)?;
        Ok(ResponseMessage {
            correlation_id,
            middleware,
            response,
        })
    }

    pub fn decode_from_file<H: AsRef<Path>>(
        file_name: H,
        version: Version,
    ) -> Result<Self, IoError> {
        debug!("decoding from file: {:#?}", file_name.as_ref());
        let mut f = File::open(file_name)?;
        let mut buffer: [u8; 1000] = [0; 1000];

        f.read_exact(&mut buffer)?;
        let data = buffer.to_vec();

        let mut src = Cursor::new(&data);

        // ResponseMessage implementation of fluvio_protocol::storage::FileWrite trait first encodes the length
        // of the ResponseMessage
        let mut size: i32 = 0;
        size.decode(&mut src, version)?;
        trace!("decoded response size: {} bytes", size);

        if src.remaining() < size as usize {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "not enought for response",
            ));
        }
        Self::decode_from(&mut src, version)
    }
}
*/

impl<P, M> Encoder for ResponseMessage<P, M>
where
    P: Encoder + Default,
    M: RequestMiddleWare,
{
    fn write_size(&self, version: Version) -> usize {
        self.correlation_id.write_size(version) + self.response.write_size(version)
    }

    fn encode<T>(&self, out: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        let len = self.write_size(version);
        trace!(
            "encoding kf response: {} version: {}, len: {}",
            std::any::type_name::<P>(),
            version,
            len
        );
        self.correlation_id.encode(out, version)?;
        self.response.encode(out, version)?;
        Ok(())
    }
}
