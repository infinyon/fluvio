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

#[derive(Debug, Default)]
pub struct ResponseMessage<P> {
    pub correlation_id: i32,
    pub response: P,
}

impl<P> ResponseMessage<P> {
    #[allow(unused)]
    pub fn from_header(header: &RequestHeader, response: P) -> Self {
        Self::new(header.correlation_id(), response)
    }

    pub fn new(correlation_id: i32, response: P) -> Self {
        Self {
            correlation_id,
            response,
        }
    }
}

impl<P> ResponseMessage<P>
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
                "not enough for response",
            ));
        }
        Self::decode_from(&mut src, version)
    }
}

impl<P> Encoder for ResponseMessage<P>
where
    P: Encoder + Default,
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
