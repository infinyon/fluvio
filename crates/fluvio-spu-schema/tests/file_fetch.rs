use std::io::Error as IoError;
use std::env::temp_dir;
use std::time::Duration;

use tracing::debug;
use futures_util::io::AsyncWriteExt;
use futures_util::future::join;
use futures_util::stream::StreamExt;

use fluvio_future::timer::sleep;
use fluvio_future::fs::util as file_util;
use fluvio_future::fs::AsyncFileExtension;
use fluvio_future::net::TcpListener;
use fluvio_protocol::Encoder;
use fluvio_protocol::api::{Request, ResponseMessage, RequestMessage};
use fluvio_protocol::record::Batch;
use fluvio_protocol::record::Record;
use fluvio_spu_schema::fetch::{
    DefaultFetchRequest, FileFetchResponse, FileFetchRequest, FilePartitionResponse,
    FileTopicResponse,
};
use fluvio_socket::SocketError;

use flv_util::fixture::ensure_clean_file;
use fluvio_socket::FluvioSocket;

/// create sample batches with message
fn create_batches(records: u16) -> Batch {
    let mut batches = Batch::default();
    let header = batches.get_mut_header();
    header.magic = 2;
    header.producer_id = 20;
    header.producer_epoch = -1;

    for i in 0..records {
        let key = format!("key {i}");
        let value = format!("value {i}");
        let record = Record::from((key, value));
        batches.add_record(record);
    }
    batches
}

async fn setup_batch_file() -> Result<(), IoError> {
    let test_file_path = temp_dir().join("batch_fetch");
    ensure_clean_file(&test_file_path);
    debug!("creating test file: {:#?}", test_file_path);
    let mut file = file_util::create(&test_file_path).await?;
    let batch = create_batches(2);
    let bytes = batch.as_bytes(0)?;
    file.write_all(bytes.as_ref()).await?;
    Ok(())
}

async fn test_server(addr: &str) -> Result<(), SocketError> {
    let listener = TcpListener::bind(addr).await?;
    debug!("server is running");
    let mut incoming = listener.incoming();
    let incoming_stream = incoming.next().await;
    debug!("server: got connection");
    let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
    let mut socket: FluvioSocket = incoming_stream.into();

    let fetch_request: Result<RequestMessage<FileFetchRequest>, SocketError> = socket
        .get_mut_stream()
        .next_request_item()
        .await
        .expect("next value");
    let request = fetch_request?;
    debug!("received fetch request: {:#?}", request);

    let test_file_path = temp_dir().join("batch_fetch");
    debug!("opening file test file: {:#?}", test_file_path);
    let file = file_util::open(&test_file_path).await?;

    let mut response = FileFetchResponse::default();
    let mut topic_response = FileTopicResponse::default();
    let part_response = FilePartitionResponse {
        partition_index: 10,
        records: file.as_slice(0, None).await?.into(),
        ..Default::default()
    };
    topic_response.partitions.push(part_response);
    response.topics.push(topic_response);
    let resp_msg = ResponseMessage::new(10, response);

    debug!(
        "response message write size: {}",
        resp_msg.write_size(FileFetchRequest::DEFAULT_API_VERSION)
    );

    socket
        .get_mut_sink()
        .encode_file_slices(&resp_msg, FileFetchRequest::DEFAULT_API_VERSION)
        .await?;
    debug!("server: finish sending out");
    Ok(())
}

async fn setup_client(addr: &str) -> Result<(), SocketError> {
    sleep(Duration::from_millis(50)).await;
    debug!("client: trying to connect");
    let mut socket = FluvioSocket::connect(addr).await?;
    debug!("client: connect to test server and waiting...");

    let req_msg: RequestMessage<DefaultFetchRequest> = RequestMessage::default();
    let res_msg = socket.send(&req_msg).await?;

    debug!("output: {:#?}", res_msg);
    let topic_responses = res_msg.response.topics;
    assert_eq!(topic_responses.len(), 1);
    let part_responses = &topic_responses[0].partitions;
    assert_eq!(part_responses.len(), 1);
    let batches = &part_responses[0].records.batches;
    assert_eq!(batches.len(), 1);
    let records = &batches[0].records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].value().to_string(), "value 0");
    assert_eq!(records[0].key().as_ref().unwrap().to_string(), "key 0");
    assert_eq!(records[1].value().to_string(), "value 1");
    assert_eq!(records[1].key().as_ref().unwrap().to_string(), "key 1");

    Ok(())
}

/// test server where it is sending out file copy
#[fluvio_future::test]
async fn test_save_fetch() {
    // create fetch and save
    setup_batch_file().await.expect("setup");

    let addr = "127.0.0.1:9911";

    let _r = join(setup_client(addr), test_server(addr)).await;
}
