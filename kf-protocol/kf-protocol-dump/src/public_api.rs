use std::io::ErrorKind;
use std::io::Error as IoError;
use std::convert::TryInto;

use log::trace;

use kf_protocol::bytes::Buf;
use kf_protocol::Decoder;
use kf_protocol_api::KfRequestMessage;
use kf_protocol_api::RequestMessage;
use kf_protocol_api::RequestHeader;

use kf_protocol_message::api_versions::KfApiVersionsRequest;
use kf_protocol_message::produce::DefaultKfProduceRequest;
use kf_protocol_message::fetch::KfFetchRequest;
use kf_protocol_message::fetch::DefaultKfFetchRequest;
use kf_protocol_message::group::KfJoinGroupRequest;
use kf_protocol_message::metadata::KfUpdateMetadataRequest;
use kf_protocol_api::api_decode;
use kf_protocol_api::AllKfApiKey;

#[derive(Debug)]
pub enum PublicRequest {
    KfApiVersionsRequest(RequestMessage<KfApiVersionsRequest>),
    KfProduceRequest(RequestMessage<DefaultKfProduceRequest>),
    KfFetchRequest(RequestMessage<DefaultKfFetchRequest>),
    KfJoinGroupRequest(RequestMessage<KfJoinGroupRequest>),
    KfUpdateMetadataRequest(RequestMessage<KfUpdateMetadataRequest>),
}

impl Default for PublicRequest {
    fn default() -> PublicRequest {
        PublicRequest::KfApiVersionsRequest(RequestMessage::<KfApiVersionsRequest>::default())
    }
}

impl KfRequestMessage for PublicRequest {
    type ApiKey = AllKfApiKey;

    fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        trace!("decoding with header: {:#?}", header);
        match header.api_key().try_into()? {
            AllKfApiKey::ApiVersion => {
                api_decode!(PublicRequest, KfApiVersionsRequest, src, header)
            }
            AllKfApiKey::Produce => {
                let request = DefaultKfProduceRequest::decode_from(src, header.api_version())?;
                Ok(PublicRequest::KfProduceRequest(RequestMessage::new(
                    header, request,
                )))
            }
            AllKfApiKey::Fetch => api_decode!(PublicRequest, KfFetchRequest, src, header),
            AllKfApiKey::JoinGroup => api_decode!(PublicRequest, KfJoinGroupRequest, src, header),
            AllKfApiKey::UpdateMetadata => {
                api_decode!(PublicRequest, KfUpdateMetadataRequest, src, header)
            }
            _ => Err(IoError::new(
                ErrorKind::Other,
                "trying to decoded unrecog api",
            )),
        }
    }
}
