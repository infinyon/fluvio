// ApiRequest and Response that has all request and response
// use for generic dump and client

use std::convert::TryInto;
use std::io::Error as IoError;
use std::fmt;

use tracing::trace;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Decoder;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::api::api_decode;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;

use crate::produce::DefaultProduceRequest;
use crate::fetch::FileFetchRequest;
use crate::ApiVersionsRequest;

use super::SpuServerApiKey;
use super::fetch_offset::FetchOffsetsRequest;
use super::stream_fetch::FileStreamFetchRequest;
use super::consumer_offset::{
    UpdateConsumerOffsetRequest, DeleteConsumerOffsetRequest, FetchConsumerOffsetsRequest,
};
use super::update_offset::UpdateOffsetsRequest;
use super::mirror::StartMirrorRequest;

#[allow(clippy::large_enum_variant)]
/// Request to Spu Server
#[derive(Debug)]
pub enum SpuServerRequest {
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),

    ProduceRequest(RequestMessage<DefaultProduceRequest>),
    FileFetchRequest(RequestMessage<FileFetchRequest>),
    FetchOffsetsRequest(RequestMessage<FetchOffsetsRequest>),
    FileStreamFetchRequest(RequestMessage<FileStreamFetchRequest>),
    UpdateOffsetsRequest(RequestMessage<UpdateOffsetsRequest>),
    UpdateConsumerOffsetRequest(RequestMessage<UpdateConsumerOffsetRequest>),
    DeleteConsumerOffsetRequest(RequestMessage<DeleteConsumerOffsetRequest>),
    FetchConsumerOffsetsRequest(RequestMessage<FetchConsumerOffsetsRequest>),
    StartMirrorRequest(RequestMessage<StartMirrorRequest>),
}

impl fmt::Display for SpuServerRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ApiVersionsRequest(_) => write!(f, "ApiVersionsRequest"),
            Self::ProduceRequest(_) => write!(f, "ProduceRequest"),
            Self::FileFetchRequest(_) => write!(f, "FileFetchRequest"),
            Self::FetchOffsetsRequest(_) => write!(f, "FetchOffsetsRequest"),
            Self::FileStreamFetchRequest(_) => write!(f, "FileStreamFetchRequest"),
            Self::UpdateOffsetsRequest(_) => write!(f, "UpdateOffsetsRequest"),
            Self::UpdateConsumerOffsetRequest(_) => write!(f, "UpdateConsumerOffsetRequest"),
            Self::DeleteConsumerOffsetRequest(_) => write!(f, "DeleteConsumerOffsetRequest"),
            Self::FetchConsumerOffsetsRequest(_) => write!(f, "FetchConsumerOffsetsRequest"),
            Self::StartMirrorRequest(_) => write!(f, "StartMirrorRequest"),
        }
    }
}

impl Default for SpuServerRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl ApiMessage for SpuServerRequest {
    type ApiKey = SpuServerApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        match header.api_key().try_into()? {
            // Mixed
            SpuServerApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),

            SpuServerApiKey::Produce => {
                let request = DefaultProduceRequest::decode_from(src, header.api_version())?;
                Ok(Self::ProduceRequest(RequestMessage::new(header, request)))
            }
            SpuServerApiKey::Fetch => api_decode!(Self, FileFetchRequest, src, header),
            SpuServerApiKey::FetchOffsets => api_decode!(Self, FetchOffsetsRequest, src, header),
            SpuServerApiKey::StreamFetch => api_decode!(Self, FileStreamFetchRequest, src, header),
            SpuServerApiKey::UpdateOffsets => api_decode!(Self, UpdateOffsetsRequest, src, header),
            SpuServerApiKey::UpdateConsumerOffset => {
                api_decode!(Self, UpdateConsumerOffsetRequest, src, header)
            }
            SpuServerApiKey::DeleteConsumerOffset => {
                api_decode!(Self, DeleteConsumerOffsetRequest, src, header)
            }
            SpuServerApiKey::FetchConsumerOffsets => {
                api_decode!(Self, FetchConsumerOffsetsRequest, src, header)
            }
            SpuServerApiKey::StartMirror => api_decode!(Self, StartMirrorRequest, src, header),
        }
    }
}
