mod create;
mod delete;
mod list;
mod watch;

pub use create::*;
pub use delete::*;
pub use list::*;
pub use watch::*;
pub use metadata::*;

pub(crate) const COMMON_VERSION: i16 = 10; // from now, we use a single version for all objects

mod metadata {

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

    use super::COMMON_VERSION;

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
    #[derive(Debug, Default, Encoder, Decoder)]
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
                let mut buf = Cursor::new(self.buf.as_ref());
                Ok(Some(O::decode_from(&mut buf, COMMON_VERSION)?))
            } else {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;

    use fluvio_protocol::api::{RequestHeader, ResponseMessage, RequestMessage};
    use fluvio_protocol::{Encoder, Decoder};
    use fluvio_protocol::api::Request;
    use fluvio_controlplane_metadata::spu::SpuStatus;

    use crate::TryEncodableFrom;
    use crate::objects::{
        Metadata, MetadataUpdate, ListResponse, ObjectApiWatchRequest, ObjectApiListResponse,
    };

    use crate::topic::TopicSpec;
    use crate::customspu::CustomSpuSpec;

    use super::{
        ListRequest, ObjectApiListRequest, WatchResponse, ObjectApiWatchResponse, COMMON_VERSION,
    };

    fn create_req() -> ObjectApiListRequest {
        let list_request: ListRequest<TopicSpec> = ListRequest::new(vec![], false);
        ObjectApiListRequest::try_encode_from(list_request, COMMON_VERSION).expect("encode")
    }

    fn create_res() -> ObjectApiWatchResponse {
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);
        ObjectApiWatchResponse::try_encode_from(watch_response, COMMON_VERSION).expect("encode")
    }

    #[test]
    fn test_from() {
        let req = create_req();
        assert!((req.downcast().expect("downcast") as Option<ListRequest<TopicSpec>>).is_some());
    }

    #[test]
    fn test_encode_decoding() {
        use fluvio_protocol::api::Request;

        let req = create_req();

        let mut req_msg = RequestMessage::new_request(req);
        req_msg
            .get_mut_header()
            .set_client_id("test")
            .set_api_version(ObjectApiListRequest::API_KEY as i16);

        let mut src = vec![];
        req_msg.encode(&mut src, 0).expect("encoding");

        let dec_msg: RequestMessage<ObjectApiListRequest> = RequestMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiListRequest::API_KEY as i16,
        )
        .expect("decode");
        assert!(
            (dec_msg.request.downcast().expect("downcast") as Option<ListRequest<TopicSpec>>)
                .is_some()
        );
    }

    // test encoding and decoding of metadata update
    #[test]
    fn test_watch_response_encoding() {
        fluvio_future::subscriber::init_logger();
        let update = MetadataUpdate {
            epoch: 2,
            changes: vec![],
            all: vec![],
        };
        let watch_response: WatchResponse<TopicSpec> = WatchResponse::new(update);

        let mut src = vec![];
        watch_response
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");
        //watch_response.encode(&mut src, 0).expect("encoding");
        println!("output: {src:#?}");
        let dec = WatchResponse::<TopicSpec>::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        assert_eq!(dec.inner().epoch, 2);
    }

    #[test]
    fn test_obj_watch_response_encode_decoding() {
        fluvio_future::subscriber::init_logger();

        let res = create_res();

        let mut header = RequestHeader::new(ObjectApiWatchRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, res);

        let mut src = vec![];
        res_msg
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");

        println!("output: {src:#?}");

        assert_eq!(
            src.len(),
            res_msg.write_size(ObjectApiWatchRequest::API_KEY as i16)
        );

        let dec_msg: ResponseMessage<ObjectApiWatchResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        let _ = (dec_msg.response.downcast().expect("downcast")
            as Option<WatchResponse<TopicSpec>>)
            .unwrap();
    }

    #[test]
    fn test_obj_watch_api_decoding() {
        fluvio_future::subscriber::init_logger();

        let res = create_res();

        let mut header = RequestHeader::new(ObjectApiWatchRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, res);

        let mut src = vec![];
        res_msg
            .encode(&mut src, ObjectApiWatchRequest::API_KEY as i16)
            .expect("encoding");

        println!("output: {src:#?}");

        assert_eq!(
            src.len(),
            res_msg.write_size(ObjectApiWatchRequest::API_KEY as i16)
        );

        let dec_msg: ResponseMessage<ObjectApiWatchResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiWatchRequest::API_KEY as i16,
        )
        .expect("decode");
        let _ = (dec_msg.response.downcast().expect("downcast")
            as Option<WatchResponse<TopicSpec>>)
            .unwrap();
    }

    #[test]
    fn test_list_response_encode_decoding() {
        use fluvio_protocol::api::Request;

        fluvio_future::subscriber::init_logger();

        let list = ListResponse::<CustomSpuSpec>::new(vec![Metadata {
            name: "test".to_string(),
            spec: CustomSpuSpec::default(),
            status: SpuStatus::default(),
        }]);

        let resp = ObjectApiListResponse::try_encode_from(list, COMMON_VERSION).expect("encode");

        let mut header = RequestHeader::new(ObjectApiListRequest::API_KEY);
        header.set_client_id("test");
        header.set_correlation_id(11);
        let res_msg = ResponseMessage::from_header(&header, resp);
        let mut src = vec![];
        res_msg.encode(&mut src, 0).expect("encoding");

        println!("output: {src:#?}");

        let dec_msg: ResponseMessage<ObjectApiListResponse> = ResponseMessage::decode_from(
            &mut Cursor::new(&src),
            ObjectApiListRequest::API_KEY as i16,
        )
        .expect("decode");

        let response = (dec_msg.response.downcast().expect("downcast")
            as Option<ListResponse<CustomSpuSpec>>)
            .unwrap();
        assert_eq!(response.inner().len(), 1);
    }
}
