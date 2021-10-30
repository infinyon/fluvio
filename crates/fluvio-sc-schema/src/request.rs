//!
//! # API Requests
//!
//! Maps SC Api Requests with their associated Responses.
//!

use std::convert::{TryInto};
use std::io::Error as IoError;
use std::fmt::Debug;

use tracing::{debug};

use dataplane::bytes::{Buf};
use dataplane::api::{ApiMessage, RequestHeader, RequestMessage};

use dataplane::api::api_decode;
use dataplane::core::{Decoder};
use dataplane::versions::ApiVersionsRequest;

use crate::AdminPublicApiKey;
use crate::objects::{
    ObjectApiListRequest, ObjectApiCreateRequest, ObjectApiWatchRequest, ObjectApiDeleteRequest,
};
use crate::{CreateDecoder, ObjectDecoder};

/// Non generic AdminRequest, This is typically used Decoding
#[derive(Debug)]
pub enum AdminPublicDecodedRequest {
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),
    CreateRequest(RequestMessage<ObjectApiCreateRequest>),
    DeleteRequest(RequestMessage<ObjectApiDeleteRequest>),
    ListRequest(RequestMessage<ObjectApiListRequest>),
    WatchRequest(RequestMessage<ObjectApiWatchRequest>),
}

impl Default for AdminPublicDecodedRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl ApiMessage for AdminPublicDecodedRequest {
    type ApiKey = AdminPublicApiKey;

    fn decode_with_header<T>(_src: &mut T, _header: RequestHeader) -> Result<Self, IoError>
    where
        Self: Default + Sized,
        Self::ApiKey: Sized,
        T: Buf,
    {
        panic!("not needed")
    }

    fn decode_from<T>(src: &mut T) -> Result<Self, IoError>
    where
        T: Buf,
    {
        let header = RequestHeader::decode_from(src, 0)?;

        let api_key = header.api_key().try_into()?;
        debug!(
            "decoding admin public request from: {} api: {:#?}",
            header.client_id(),
            api_key
        );
        match api_key {
            AdminPublicApiKey::ApiVersion => api_decode!(Self, ApiVersionsRequest, src, header),
            AdminPublicApiKey::Create => Ok(Self::CreateRequest(RequestMessage::<
                ObjectApiCreateRequest,
            >::decode_with_header(
                src, header
            )?)),
            AdminPublicApiKey::Delete => Ok(Self::DeleteRequest(RequestMessage::<
                ObjectApiDeleteRequest,
            >::decode_with_header(
                src, header
            )?)),

            AdminPublicApiKey::List => Ok(Self::ListRequest(RequestMessage::<
                ObjectApiListRequest,
            >::decode_with_header(
                src, header
            )?)),

            AdminPublicApiKey::Watch => Ok(Self::WatchRequest(RequestMessage::<
                ObjectApiWatchRequest,
            >::decode_with_header(
                src, header
            )?)),
        }
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;

    use dataplane::api::RequestMessage;
    use dataplane::core::{Encoder};
    use dataplane::api::ApiMessage;

    use crate::objects::{ListRequest, ObjectApiListRequest};
    use crate::{AdminPublicDecodedRequest, ObjectDecoder};
    use crate::topic::TopicSpec;

    fn create_req() -> (ObjectApiListRequest, ObjectDecoder) {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![]);
        list_request.into()
    }

    #[test]
    fn test_list_encode_decoding() {
        use dataplane::api::Request;

        let (list_req, mw) = create_req();

        let mut req_msg = RequestMessage::request_with_mw(list_req, mw);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiListRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_req: AdminPublicDecodedRequest =
            AdminPublicDecodedRequest::decode_from(&mut Cursor::new(&src)).expect("decode");

        assert!(matches!(dec_req, AdminPublicDecodedRequest::ListRequest(_)));
    }
}
