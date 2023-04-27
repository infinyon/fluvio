use std::io::Cursor;

use fluvio_protocol::api::{RequestHeader, ResponseMessage, RequestMessage};
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_controlplane_metadata::spu::SpuStatus;

use crate::TryEncodableFrom;

use crate::objects::{
    Metadata, MetadataUpdate, ListResponse, ObjectApiWatchRequest, ObjectApiListResponse,
    ClassicObjectApiListRequest,
};

use crate::topic::TopicSpec;
use crate::customspu::CustomSpuSpec;

use super::{ListRequest, ObjectApiListRequest, WatchResponse, ObjectApiWatchResponse, COMMON_VERSION};

#[test]
fn test_encoding_compatibility() {
    let raw_req: ListRequest<TopicSpec> = ListRequest::new("test", false);
    // upcast
    let list_request =
        ObjectApiListRequest::try_encode_from(raw_req, COMMON_VERSION - 1).expect("encoded");
    let mut new_dest = vec![];
    list_request
        .encode(&mut new_dest, COMMON_VERSION - 1)
        .expect("encoding");

    let raw_req2: ListRequest<TopicSpec> = ListRequest::new("test", false);
    let old_topic_request = ClassicObjectApiListRequest::Topic(raw_req2);
    let mut old_dest: Vec<u8> = vec![];
    old_topic_request
        .encode(&mut old_dest, COMMON_VERSION - 1)
        .expect("encoding");

    //  assert_eq!(new_dest.len(),20);
    assert_eq!(old_dest, new_dest);
}

// encoding and decoding request
#[test]
fn test_req_encoding_decoding() {
    let raw_req: ListRequest<TopicSpec> = ListRequest::new("test", false);

    let test_request =
        ObjectApiListRequest::try_encode_from(raw_req, COMMON_VERSION).expect("encoded");
    let mut dest = vec![];
    test_request
        .encode(&mut dest, COMMON_VERSION)
        .expect("encoding");

    let recovered_request =
        ObjectApiListRequest::decode_from(&mut Cursor::new(dest), COMMON_VERSION).expect("decode");

    let downcast =
        recovered_request.downcast().expect("downcast") as Option<ListRequest<TopicSpec>>;
    assert!(downcast.is_some());
}

#[test]
fn test_req_old_to_new() {
    let raw_req: ListRequest<TopicSpec> = ListRequest::new(vec![], false);

    let old_topic_request = ClassicObjectApiListRequest::Topic(raw_req);
    let mut dest = vec![];
    old_topic_request
        .encode(&mut dest, COMMON_VERSION)
        .expect("encoding");

    let new_topic_request =
        ObjectApiListRequest::decode_from(&mut Cursor::new(dest), COMMON_VERSION - 1)
            .expect("decode");

    let downcast =
        new_topic_request.downcast().expect("downcast") as Option<ListRequest<TopicSpec>>;
    assert!(downcast.is_some());
}

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
fn test_encode_decoding_dynamic() {
    use fluvio_protocol::api::Request;

    let req = create_req();

    let mut req_msg = RequestMessage::new_request(req);
    req_msg
        .get_mut_header()
        .set_client_id("test")
        .set_api_version(ObjectApiListRequest::API_KEY as i16);

    let mut src = vec![];
    req_msg.encode(&mut src, 0).expect("encoding");

    let dec_msg: RequestMessage<ObjectApiListRequest> =
        RequestMessage::decode_from(&mut Cursor::new(&src), ObjectApiListRequest::API_KEY as i16)
            .expect("decode");
    assert!(
        (dec_msg.request.downcast().expect("downcast") as Option<ListRequest<TopicSpec>>).is_some()
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
    let _ = (dec_msg.response.downcast().expect("downcast") as Option<WatchResponse<TopicSpec>>)
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
    let _ = (dec_msg.response.downcast().expect("downcast") as Option<WatchResponse<TopicSpec>>)
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
    res_msg.encode(&mut src, COMMON_VERSION).expect("encoding");

    println!("output: {src:#?}");

    let dec_msg: ResponseMessage<ObjectApiListResponse> =
        ResponseMessage::decode_from(&mut Cursor::new(&src), COMMON_VERSION).expect("decode");

    let response = (dec_msg.response.downcast().expect("downcast")
        as Option<ListResponse<CustomSpuSpec>>)
        .unwrap();
    assert_eq!(response.inner().len(), 1);
}
