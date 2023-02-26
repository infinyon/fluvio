use std::io::Error as IoError;

use tracing::trace;

use bytes::{Buf, BufMut};
use crate::DecodeExt;
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

impl<P> DecodeExt for ResponseMessage<P>
where
    P: DecodeExt,
{
    fn decode_from<T>(src: &mut T, version: Version) -> Result<Self, IoError>
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
