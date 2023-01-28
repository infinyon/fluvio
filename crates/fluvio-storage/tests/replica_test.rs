// test fetch of replica

use std::{env::temp_dir, sync::Arc};
use std::time::Duration;

use fluvio_spu_schema::Isolation;
use tracing::debug;
use futures_lite::StreamExt;
use futures_lite::future::zip;

use fluvio_future::timer::sleep;
use fluvio_future::net::TcpListener;
use fluvio_spu_schema::{
    fetch::{
        FetchPartition, FetchableTopic, DefaultFetchRequest, FileFetchResponse, FileFetchRequest,
        FilePartitionResponse, FileTopicResponse,
    },
};
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::record::{Record, RecordSet};
use fluvio_protocol::record::Offset;
use fluvio_protocol::fixture::BatchProducer;

use fluvio_socket::{FluvioSocket, SocketError};
use flv_util::fixture::ensure_clean_dir;
use fluvio_storage::{StorageError, ReplicaStorage, FileReplica};
use fluvio_storage::config::ReplicaConfig;
use fluvio_storage::fixture::storage_config;

const TEST_REP_DIR: &str = "testreplica-fetch";
const START_OFFSET: Offset = 0;
const TOPIC_NAME: &str = "testsimple";

fn default_option() -> ReplicaConfig {
    ReplicaConfig {
        segment_max_bytes: 10000,
        base_dir: temp_dir().join(TEST_REP_DIR),
        index_max_interval_bytes: 1000,
        index_max_bytes: 1000,
        ..Default::default()
    }
}

fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
    let msg = format!("record {record_index}");
    Record::new(msg)
}

/// create sample batches with variable number of records
fn create_records(records: u16) -> RecordSet {
    BatchProducer::builder()
        .records(records)
        .record_generator(Arc::new(generate_record))
        .build()
        .expect("batch")
        .records()
}

// create new replica and add two batches
async fn setup_replica() -> Result<FileReplica, StorageError> {
    let option = default_option();

    ensure_clean_dir(&option.base_dir);

    let mut replica = FileReplica::create_or_load_with_storage(
        TOPIC_NAME,
        0,
        START_OFFSET,
        option,
        storage_config(),
    )
    .await
    .expect("test replica");
    replica
        .write_recordset(&mut create_records(2), false)
        .await
        .expect("first batch");
    replica
        .write_recordset(&mut create_records(2), false)
        .await
        .expect("second batch");

    Ok(replica)
}

async fn handle_response(socket: &mut FluvioSocket, replica: &FileReplica) {
    let request: Result<RequestMessage<FileFetchRequest>, SocketError> = socket
        .get_mut_stream()
        .next_request_item()
        .await
        .expect("next value");
    let request = request.expect("request");

    let (header, fetch_request) = request.get_header_request();
    debug!("server: received fetch request");

    let topic_request = &fetch_request.topics[0];
    let partition_request = &topic_request.fetch_partitions[0];
    let fetch_offset = partition_request.fetch_offset;
    debug!("server: fetch offset: {}", fetch_offset);
    let mut response = FileFetchResponse::default();
    let mut topic_response = FileTopicResponse::default();
    let mut part_response = FilePartitionResponse::default();
    // log contains 182 bytes total
    let slice = replica
        .read_partition_slice(
            fetch_offset,
            FileReplica::PREFER_MAX_LEN,
            Isolation::ReadUncommitted,
        )
        .await
        .expect("read");
    debug!("response: {:#?}", slice);

    part_response.partition_index = 0;
    if let Some(file_slice) = slice.file_slice {
        part_response.records = file_slice.into();
    }

    // assert_eq!(part)
    topic_response.partitions.push(part_response);
    response.topics.push(topic_response);

    let response = RequestMessage::<FileFetchRequest>::response_with_header(&header, response);
    socket
        .get_mut_sink()
        .encode_file_slices(&response, 10)
        .await
        .expect("encoding");
    debug!("server: finish sending out");
}

async fn test_server(addr: &str) {
    debug!("setting up replica");
    let replica = setup_replica().await.expect("setup");

    debug!("set up the replica");
    let listener = TcpListener::bind(&addr).await.expect("bind");
    debug!("server is running");
    let mut incoming = listener.incoming();

    // listen 2 times
    for i in 0u16..1 {
        debug!("server: waiting for client {}", i);
        let incoming_stream = incoming.next().await;
        debug!("server: got connection from client");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: FluvioSocket = incoming_stream.into();
        handle_response(&mut socket, &replica).await;
    }
}

async fn test_fetch(addr: &str, iteration: i16, offset: i64, expected_batch_len: usize) {
    let mut socket = FluvioSocket::connect(addr)
        .await
        .expect("should connect to server");

    debug!("testing fetch: {}", iteration);
    let mut request = DefaultFetchRequest::default();
    let mut topic_request = FetchableTopic {
        name: TOPIC_NAME.to_owned(),
        ..Default::default()
    };
    let part_request = FetchPartition {
        fetch_offset: offset,
        ..Default::default()
    };
    topic_request.fetch_partitions.push(part_request);
    request.topics.push(topic_request);

    let mut req_msg = RequestMessage::new_request(request);
    req_msg
        .get_mut_header()
        .set_client_id("test")
        .set_correlation_id(10);

    let res_msg = socket.send(&req_msg).await.expect("send");

    let topic_responses = res_msg.response.topics;
    assert_eq!(topic_responses.len(), 1);
    let part_responses = &topic_responses[0].partitions;
    assert_eq!(part_responses.len(), 1);

    let batches = &part_responses[0].records.batches;
    assert_eq!(batches.len(), expected_batch_len);
    let records = &batches[0].records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].value.to_string(), "record 0");
    assert_eq!(records[1].value.to_string(), "record 1");
}

async fn test_client(addr: &str) {
    sleep(Duration::from_millis(100)).await;
    // for offset 0, it should return entire batches
    test_fetch(addr, 0, 0, 2).await;

    // for offset 1, it should return only last batch
    // test_fetch(addr, 1, 2, 1).await;
}

/// test replica fetch using dummy server
#[fluvio_future::test]
async fn test_replica_fetch() {
    let addr = "127.0.0.1:9911";

    let _r = zip(test_client(addr), test_server(addr)).await;
}
