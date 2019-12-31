// test fetch of replica

use std::env::temp_dir;
use std::net::SocketAddr;
use std::time::Duration;

use log::debug;
use futures::stream::StreamExt;
use futures::future::join;

use flv_future_core::test_async;
use flv_future_core::sleep;
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::FetchableTopic;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;
use kf_protocol::message::fetch::DefaultKfFetchRequest;
use kf_protocol::api::Offset;
use flv_future_aio::net::AsyncTcpListener;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;

use utils::fixture::ensure_clean_dir;

use kf_socket::FileFetchResponse;
use kf_socket::KfFileFetchRequest;
use kf_socket::FilePartitionResponse;
use kf_socket::FileTopicResponse;
use flv_storage::StorageError;
use flv_storage::FileReplica;
use flv_storage::ConfigOption;

const TEST_REP_DIR: &str = "testreplica-fetch";
const START_OFFSET: Offset = 0;

fn default_option() -> ConfigOption {
    ConfigOption {
        segment_max_bytes: 10000,
        base_dir: temp_dir().join(TEST_REP_DIR),
        index_max_interval_bytes: 1000,
        index_max_bytes: 1000,
        ..Default::default()
    }
}

/// create sample batches with variable number of records
fn create_batch(records: u16) -> DefaultBatch {
    let mut batches = DefaultBatch::default();
    let header = batches.get_mut_header();
    header.magic = 2;
    header.producer_id = 20;
    header.producer_epoch = -1;

    for i in 0..records {
        let msg = format!("record {}", i);
        let record: DefaultRecord = msg.into();
        batches.add_record(record);
    }

    batches
}

// create new replica and add two batches
async fn setup_replica() -> Result<FileReplica, StorageError> {
    let option = default_option();

    ensure_clean_dir(&option.base_dir);

    let mut replica = FileReplica::create("testsimple", 0, START_OFFSET, &option)
        .await
        .expect("test replica");
    replica.send(create_batch(2)).await.expect("first batch");
    replica.send(create_batch(2)).await.expect("second batch");

    Ok(replica)
}

async fn handle_response(
    socket: &mut KfSocket,
    replica: &FileReplica,
) -> Result<(), KfSocketError> {
    let request: Result<RequestMessage<KfFileFetchRequest>, KfSocketError> = socket
        .get_mut_stream()
        .next_request_item()
        .await
        .expect("next value");
    let request = request?;

    let (header, fetch_request) = request.get_header_request();
    debug!("server: received fetch request");

    let topic_request = &fetch_request.topics[0];
    let partiton_request = &topic_request.fetch_partitions[0];
    let fetch_offset = partiton_request.fetch_offset;
    debug!("server: fetch offset: {}", fetch_offset);
    let mut response = FileFetchResponse::default();
    let mut topic_response = FileTopicResponse::default();
    let mut part_response = FilePartitionResponse::default();
    replica
        .read_records(fetch_offset, None, &mut part_response)
        .await;
    topic_response.partitions.push(part_response);
    response.topics.push(topic_response);

    let response = RequestMessage::<KfFileFetchRequest>::response_with_header(&header, response);
    socket
        .get_mut_sink()
        .encode_file_slices(&response, 0)
        .await?;
    debug!("server: finish sending out");
    Ok(())
}

async fn test_server(addr: SocketAddr) -> Result<(), StorageError> {
    debug!("setting up replica");
    let replica = setup_replica().await?;

    debug!("set up the replica");
    let listener = AsyncTcpListener::bind(&addr).await?;
    debug!("server is running");
    let mut incoming = listener.incoming();

    // listen 2 times
    for i in 0..2 {
        debug!("server: waiting for client {}", i);
        let incoming_stream = incoming.next().await;
        debug!("server: got connection from client");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: KfSocket = incoming_stream.into();
        handle_response(&mut socket, &replica)
            .await
            .expect("response error");
    }
    Ok(())
}

async fn test_fetch(
    addr: &SocketAddr,
    iteration: i16,
    offset: i64,
    expected_batch_len: usize,
) -> Result<(), KfSocketError> {
    let mut socket = KfSocket::connect(addr)
        .await
        .expect("should connect to server");

    debug!("testing fetch: {}", iteration);
    let mut request = DefaultKfFetchRequest::default();
    let mut topic_request = FetchableTopic::default();
    topic_request.name = "testsimple".to_owned();
    let mut part_request = FetchPartition::default();
    part_request.fetch_offset = offset;
    topic_request.fetch_partitions.push(part_request);
    request.topics.push(topic_request);

    let mut req_msg = RequestMessage::new_request(request);
    req_msg
        .get_mut_header()
        .set_client_id("test")
        .set_correlation_id(10);

    let res_msg = socket.send(&req_msg).await?;

    debug!("output: {:#?}", res_msg);
    let topic_responses = res_msg.response.topics;
    assert_eq!(topic_responses.len(), 1);
    let part_responses = &topic_responses[0].partitions;
    assert_eq!(part_responses.len(), 1);
    let batches = &part_responses[0].records.batches;
    assert_eq!(batches.len(), expected_batch_len);
    let records = &batches[0].records;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].value.to_string(), "record 0");
    assert_eq!(records[1].value.to_string(), "record 1");

    Ok(())
}

async fn test_client(addr: SocketAddr) -> Result<(), KfSocketError> {
    sleep(Duration::from_millis(100)).await;
    // for offset 0, it should return entire batches
    test_fetch(&addr, 0, 0, 2)
        .await
        .expect("test should succeed");

    // for offset 1, it should return only last batch
    test_fetch(&addr, 1, 2, 1)
        .await
        .expect("test should succeed");
    Ok(())
}

/// test replica fetch using dummy server
#[test_async]
async fn test_replica_fetch() -> Result<(), StorageError> {
    let addr = "127.0.0.1:9911".parse::<SocketAddr>().expect("parse");

    let _r = join(test_client(addr), test_server(addr.clone())).await;

    Ok(())
}
