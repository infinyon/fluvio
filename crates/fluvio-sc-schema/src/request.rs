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
use dataplane::core::{Encoder, Decoder};
use dataplane::versions::ApiVersionsRequest;

use crate::AdminPublicApiKey;
use crate::objects::{
    ObjectApiListRequest, ObjectApiCreateRequest, ObjectApiWatchRequest, ObjectApiDeleteRequest,
};
use crate::{CreateDecoder, ObjectDecoder};
use crate::core::Spec;

#[derive(Debug)]
pub enum AdminPublicRequest {
    ApiVersionsRequest(RequestMessage<ApiVersionsRequest>),
    CreateRequest(RequestMessage<ObjectApiCreateRequest, CreateDecoder>),
    DeleteRequest(RequestMessage<ObjectApiDeleteRequest, ObjectDecoder>),
    ListRequest(RequestMessage<ObjectApiListRequest, ObjectDecoder>),
    WatchRequest(RequestMessage<ObjectApiWatchRequest, ObjectDecoder>),
}

impl Default for AdminPublicRequest {
    fn default() -> Self {
        Self::ApiVersionsRequest(RequestMessage::<ApiVersionsRequest>::default())
    }
}

impl ApiMessage for AdminPublicRequest {
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
                CreateDecoder,
            >::decode_with_header(
                src, header
            )?)),

            AdminPublicApiKey::Delete => Ok(Self::DeleteRequest(RequestMessage::<
                ObjectApiDeleteRequest,
                ObjectDecoder,
            >::decode_with_header(
                src, header
            )?)),

            AdminPublicApiKey::List => Ok(Self::ListRequest(RequestMessage::<
                ObjectApiListRequest,
                ObjectDecoder,
            >::decode_with_header(
                src, header
            )?)),

            AdminPublicApiKey::Watch => Ok(Self::WatchRequest(RequestMessage::<
                ObjectApiWatchRequest,
                ObjectDecoder,
            >::decode_with_header(
                src, header
            )?)),
        }
    }
}
mod objects {

    use super::*;

    use dataplane::api::RequestMiddleWare;

    use crate::topic::TopicSpec;
    use crate::spu::{SpuSpec};
    use crate::smartmodule::SmartModuleSpec;
    use crate::partition::PartitionSpec;

    pub trait AdminObjectDecoder {
        fn is_topic(&self) -> bool;
        fn is_spu(&self) -> bool;
        fn is_partition(&self) -> bool;
        fn is_smart_module(&self) -> bool;
    }

    #[derive(Debug, Clone, Default, Encoder, Decoder)]
    pub struct ObjectDecoder {
        ty: String,
    }

    impl RequestMiddleWare for ObjectDecoder {}

    impl AdminObjectDecoder for ObjectDecoder {
        fn is_topic(&self) -> bool {
            self.ty == TopicSpec::LABEL
        }

        fn is_spu(&self) -> bool {
            self.ty == SpuSpec::LABEL
        }

        fn is_partition(&self) -> bool {
            self.ty == PartitionSpec::LABEL
        }

        fn is_smart_module(&self) -> bool {
            self.ty == SmartModuleSpec::LABEL
        }
    }

    #[repr(u8)]
    #[derive(Debug, Clone, Encoder, Decoder)]
    pub enum CreateDecoder {
        #[fluvio(tag = 0)]
        TOPIC,
        #[fluvio(tag = 1)]
        CustomSpu,
        #[fluvio(tag = 2)]
        SPG = 2,
        #[fluvio(tag = 3)]
        ManagedConnector,
        #[fluvio(tag = 4)]
        SmartModule,
        #[fluvio(tag = 5)]
        TABLE,
    }

    impl Default for CreateDecoder {
        fn default() -> Self {
            Self::TOPIC
        }
    }

    impl RequestMiddleWare for CreateDecoder {}

    impl AdminObjectDecoder for CreateDecoder {
        fn is_topic(&self) -> bool {
            matches!(self, Self::TOPIC)
        }

        fn is_spu(&self) -> bool {
            false
        }

        fn is_partition(&self) -> bool {
            false
        }

        fn is_smart_module(&self) -> bool {
            matches!(self, Self::SmartModule)
        }
    }
}
