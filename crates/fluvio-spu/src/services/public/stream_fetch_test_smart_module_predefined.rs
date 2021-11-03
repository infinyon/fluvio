use std::{
    env::temp_dir,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};
use std::io::Read;

use flate2::{Compression, bufread::GzEncoder};
use fluvio_controlplane_metadata::{
    partition::Replica,
    smartmodule::{SmartModuleWasm, SmartModuleWasmFormat},
};
use fluvio_controlplane_metadata::smartmodule::SmartModule;
use fluvio_storage::ReplicaStorage;
use flv_util::fixture::ensure_clean_dir;
use futures_util::StreamExt;

use fluvio_future::timer::sleep;
use fluvio_socket::{FluvioSocket, MultiplexerSocket};
use dataplane::{
    Isolation,
    fetch::DefaultFetchRequest,
    fixture::BatchProducer,
    record::{RecordData, Record},
};
use dataplane::fixture::{create_batch};
use dataplane::smartstream::SmartStreamType;
use fluvio_spu_schema::server::{
    stream_fetch::{SmartModuleInvocation, SmartModuleInvocationWasm},
    update_offset::{UpdateOffsetsRequest, OffsetUpdate},
};
use crate::core::GlobalContext;
use crate::config::SpuConfig;
use crate::replication::leader::LeaderReplicaState;
use crate::services::public::create_public_server;

use std::sync::Arc;

use tracing::{debug};

use dataplane::{
    ErrorCode,
    api::{RequestMessage},
    record::RecordSet,
    SmartStreamError,
};
use fluvio_spu_schema::server::stream_fetch::{DefaultStreamFetchRequest, SmartStreamKind};

static NEXT_PORT: AtomicU16 = AtomicU16::new(12200);

fn read_filter_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
    let path = filter_path.as_ref();
    let raw_buffer =
        std::fs::read(path).unwrap_or_else(|_| panic!("Unable to read file {}", path.display()));
    let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
    let mut buffer = Vec::with_capacity(raw_buffer.len());
    encoder
        .read_to_end(&mut buffer)
        .unwrap_or_else(|_| panic!("Unable to gzip file {}", path.display()));
    buffer
}

fn load_wasm_module<S: ReplicaStorage>(module_name: &str, ctx: &GlobalContext<S>) {
    let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
    let wasm_path = PathBuf::from(spu_dir)
        .parent()
        .expect("parent")
        .join(format!(
            "fluvio-smartstream/examples/target/wasm32-unknown-unknown/release/{}.wasm",
            module_name
        ));
    let wasm = read_filter_from_path(wasm_path);
    ctx.smart_module_localstore().insert(SmartModule {
        name: module_name.to_owned(),
        wasm: SmartModuleWasm {
            format: SmartModuleWasmFormat::Binary,
            payload: wasm,
        },
        ..Default::default()
    });
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_filter() {
    let test_path = temp_dir().join("test_stream_fetch_filter");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions

    let topic = "testfilter";

    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    load_wasm_module("fluvio_wasm_filter", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter".to_owned()),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    // 1 out of 2 are filtered
    let mut records = create_filter_records(2);
    //debug!("records: {:#?}", records);
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    debug!("first filter fetch");
    let response = stream.next().await.expect("first").expect("response");
    //debug!("respose: {:#?}", response);
    let stream_id = response.stream_id;
    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 2);
        assert_eq!(partition.next_offset_for_fetch(), Some(2)); // shoule be same as HW

        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 1);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "a".repeat(100).as_bytes()
        );
        assert_eq!(batch.records()[0].get_offset_delta(), 1);
    }

    drop(response);

    // firt write 2 non filterable records
    let mut records = RecordSet::default().add(create_batch());
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    // another 1 of 3, here base offset should be = 4
    let mut records = create_filter_records(3);
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    // create another 4, base should be 4 + 3 = 7 and total 10 records
    let mut records = create_filter_records(3);
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");
    assert_eq!(replica.hw(), 10);

    debug!("2nd filter batch, hw=10");
    // consumer can send back to same offset to read back again
    debug!("send back offset ack to SPU");
    client_socket
        .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
            offsets: vec![OffsetUpdate {
                offset: 2,
                session_id: stream_id,
            }],
        }))
        .await
        .expect("send offset");

    let response = stream.next().await.expect("2nd").expect("response");
    {
        debug!("received 2nd message");
        assert_eq!(response.topic, topic);
        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 10);
        assert_eq!(partition.next_offset_for_fetch(), Some(10)); // shoule be same as HW

        // we got whole batch rather than individual batches
        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 4); // first base offset where we had filtered records
        assert_eq!(batch.records().len(), 2);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "a".repeat(100).as_bytes()
        );
    }

    drop(response);

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_filter_individual() {
    let test_path = temp_dir().join("test_stream_fetch_filter_individual");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let topic = "testfilter";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    load_wasm_module("fluvio_wasm_filter_odd", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter_odd".to_owned()),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    // First, open the consumer stream
    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let mut records: RecordSet = BatchProducer::builder()
        .records(1u16)
        .record_generator(Arc::new(|_, _| Record::new("1")))
        .build()
        .expect("batch")
        .records();
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    tokio::select! {
        _ = stream.next() => panic!("Should not receive response here"),
        _ = fluvio_future::timer::sleep(std::time::Duration::from_millis(1000)) => (),
    }

    let mut records: RecordSet = BatchProducer::builder()
        .records(1u16)
        .record_generator(Arc::new(|_, _| Record::new("2")))
        .build()
        .expect("batch")
        .records();
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    let response = stream.next().await.expect("first").expect("response");
    let records = response.partition.records.batches[0].records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.as_ref(), "2".as_bytes());

    match response.partition.error_code {
        ErrorCode::None => (),
        _ => panic!("Should not have gotten an error"),
    }

    drop(response);

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_filter_error_fetch() {
    let test_path = temp_dir().join("test_stream_filter_error_fetch");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions

    let topic = "test_filter_error";

    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    load_wasm_module("fluvio_wasm_filter_odd", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter_odd".to_owned()),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
        let value = if record_index < 10 {
            record_index.to_string()
        } else {
            "ten".to_string()
        };

        Record::new(value)
    }

    let mut records: RecordSet = BatchProducer::builder()
        .records(11u16)
        .record_generator(Arc::new(generate_record))
        .build()
        .expect("batch")
        .records();

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    debug!("first filter fetch");
    let response = stream.next().await.expect("first").expect("response");

    assert_eq!(response.partition.records.batches.len(), 1);
    let records = response.partition.records.batches[0].records();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].value.as_ref(), "0".as_bytes());
    assert_eq!(records[1].value.as_ref(), "2".as_bytes());
    assert_eq!(records[2].value.as_ref(), "4".as_bytes());
    assert_eq!(records[3].value.as_ref(), "6".as_bytes());
    assert_eq!(records[4].value.as_ref(), "8".as_bytes());

    match &response.partition.error_code {
        ErrorCode::SmartStreamError(SmartStreamError::Runtime(error)) => {
            assert_eq!(error.offset, 10);
            assert!(error.record_key.is_none());
            assert_eq!(error.record_value.as_ref(), "ten".as_bytes());
            assert_eq!(error.kind, SmartStreamType::Filter);
            let rendered = format!("{}", error);
            assert_eq!(rendered, "Oops something went wrong\n\nCaused by:\n   0: Failed to parse int\n   1: invalid digit found in string\n\nSmartStream Info: \n    Type: Filter\n    Offset: 10\n    Key: NULL\n    Value: ten");
        }
        _ => panic!("should have gotten error code"),
    }

    drop(response);

    server_end_event.notify();
    debug!("terminated controller");
}

fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
    let msg = match record_index {
        0 => "b".repeat(100),
        1 => "a".repeat(100),
        _ => "z".repeat(100),
    };

    Record::new(RecordData::from(msg))
}

/// create records that can be filtered
fn create_filter_records(records: u16) -> RecordSet {
    BatchProducer::builder()
        .records(records)
        .record_generator(Arc::new(generate_record))
        .build()
        .expect("batch")
        .records()
}

/// test filter with max bytes
#[fluvio_future::test(ignore)]
async fn test_stream_filter_max() {
    let test_path = temp_dir().join("test_stream_filter_max");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions

    let topic = "testfilter";

    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // write 2 batches each with 10 records
    //debug!("records: {:#?}", records);
    replica
        .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
        .await
        .expect("write"); // 1000 bytes
    replica
        .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
        .await
        .expect("write"); // 2000 bytes totals
    replica
        .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
        .await
        .expect("write"); // 3000 bytes total
                          // now total of 300 filter records bytes (min), but last filter record is greater than max

    load_wasm_module("fluvio_wasm_filter", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter".to_owned()),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 250,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let response = stream.next().await.expect("first").expect("response");
    debug!("respose: {:#?}", response);

    // received partial because we exceed max bytes
    let stream_id = response.stream_id;
    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 30);
        assert_eq!(partition.next_offset_for_fetch(), Some(20)); // shoule be same as HW

        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 2);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "a".repeat(100).as_bytes()
        );
    }

    drop(response);

    // consumer can send back to same offset to read back again
    debug!("send back offset ack to SPU");
    client_socket
        .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
            offsets: vec![OffsetUpdate {
                offset: 20,
                session_id: stream_id,
            }],
        }))
        .await
        .expect("send offset");

    let response = stream.next().await.expect("2nd").expect("response");
    {
        debug!("received 2nd message");
        assert_eq!(response.topic, topic);
        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 30);
        assert_eq!(partition.next_offset_for_fetch(), Some(30)); // shoule be same as HW

        // we got whole batch rather than individual batches
        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 20);
        assert_eq!(batch.records().len(), 1);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "a".repeat(100).as_bytes()
        );
    }

    drop(response);

    server_end_event.notify();
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_map_error() {
    let test_path = temp_dir().join("test_stream_fetch_map_error");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions

    let topic = "test_map_error";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    load_wasm_module("fluvio_wasm_map_double", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_map_double".to_owned()),
        kind: SmartStreamKind::Map,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let mut records: RecordSet = BatchProducer::builder()
        .records(10u16)
        .record_generator(Arc::new(|i, _| {
            if i < 9 {
                Record::new(i.to_string())
            } else {
                Record::new("nine".to_string())
            }
        }))
        .build()
        .expect("batch")
        .records();

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    debug!("first map fetch");
    let response = stream.next().await.expect("first").expect("response");

    assert_eq!(response.partition.records.batches.len(), 1);
    let records = response.partition.records.batches[0].records();
    assert_eq!(records.len(), 9);
    assert_eq!(records[0].value.as_ref(), "0".as_bytes());
    assert_eq!(records[1].value.as_ref(), "2".as_bytes());
    assert_eq!(records[2].value.as_ref(), "4".as_bytes());
    assert_eq!(records[3].value.as_ref(), "6".as_bytes());
    assert_eq!(records[4].value.as_ref(), "8".as_bytes());
    assert_eq!(records[5].value.as_ref(), "10".as_bytes());
    assert_eq!(records[6].value.as_ref(), "12".as_bytes());
    assert_eq!(records[7].value.as_ref(), "14".as_bytes());
    assert_eq!(records[8].value.as_ref(), "16".as_bytes());

    match &response.partition.error_code {
        ErrorCode::SmartStreamError(SmartStreamError::Runtime(error)) => {
            assert_eq!(error.offset, 9);
            assert_eq!(error.kind, SmartStreamType::Map);
            assert_eq!(error.record_value.as_ref(), "nine".as_bytes());
        }
        _ => panic!("should get runtime error"),
    }

    drop(response);

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_aggregate_fetch_single_batch() {
    let test_path = temp_dir().join("aggregate_stream_fetch");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let topic = "testaggregate";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // Providing an accumulator causes SPU to run aggregator
    load_wasm_module("fluvio_wasm_aggregate", &ctx);

    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_aggregate".to_owned()),
        kind: SmartStreamKind::Aggregate {
            accumulator: Vec::from("A"),
        },
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    // Aggregate 5 records
    // These records look like:
    //
    // 1
    // 2
    // 3
    // 4
    // 5
    let mut records = BatchProducer::builder()
        .records(5u16)
        .record_generator(Arc::new(|i, _| Record::new(i.to_string())))
        .build()
        .expect("batch")
        .records();
    debug!("records: {:#?}", records);

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    debug!("first aggregate fetch");
    let response = stream.next().await.expect("first").expect("response");
    let stream_id = response.stream_id;

    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 5);
        assert_eq!(partition.next_offset_for_fetch(), Some(5)); // shoule be same as HW

        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 5);

        let records = batch.records();

        assert_eq!("A0", records[0].value().as_str().expect("string"));
        assert_eq!("A01", records[1].value().as_str().expect("string"));
        assert_eq!("A012", records[2].value().as_str().expect("string"));
        assert_eq!("A0123", records[3].value().as_str().expect("string"));
        assert_eq!("A01234", records[4].value().as_str().expect("string"));
    }

    // consumer can send back to same offset to read back again
    debug!("send back offset ack to SPU");
    client_socket
        .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
            offsets: vec![OffsetUpdate {
                offset: 20,
                session_id: stream_id,
            }],
        }))
        .await
        .expect("send offset");

    server_end_event.notify();
}

#[fluvio_future::test(ignore)]
async fn test_stream_aggregate_fetch_multiple_batch() {
    let test_path = temp_dir().join("aggregate_stream_fetch");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let topic = "testaggregatebatch";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // Aggregate 5 records
    // These records look like:
    //
    // 1
    // 2
    // 3
    // 4
    // 5
    let mut records = BatchProducer::builder()
        .records(1u16)
        .record_generator(Arc::new(|_, _| Record::new("0")))
        .build()
        .expect("batch")
        .records();
    debug!("first batch: {:#?}", records);

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    let mut records2 = BatchProducer::builder()
        .records(1u16)
        .record_generator(Arc::new(|_, _| Record::new("1")))
        .build()
        .expect("batch")
        .records();

    debug!("2nd batch: {:#?}", records2);

    replica
        .write_record_set(&mut records2, ctx.follower_notifier())
        .await
        .expect("write");

    // Providing an accumulator causes SPU to run aggregator
    load_wasm_module("fluvio_wasm_aggregate", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_aggregate".to_owned()),
        kind: SmartStreamKind::Aggregate {
            accumulator: Vec::from("A"),
        },
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    debug!("first aggregate fetch");
    let response = stream.next().await.expect("first").expect("response");
    let stream_id = response.stream_id;

    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 2);
        assert_eq!(partition.next_offset_for_fetch(), Some(2)); // shoule be same as HW

        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 2);

        let records = batch.records();

        assert_eq!("A0", records[0].value().as_str().expect("string"));
        assert_eq!("A1", records[1].value().as_str().expect("string"));
        //   assert_eq!("A2", records[2].value().as_str().expect("string"));
        //   assert_eq!("A3", records[3].value().as_str().expect("string"));
        //   assert_eq!("A4", records[4].value().as_str().expect("string"));
    }

    // consumer can send back to same offset to read back again
    debug!("send back offset ack to SPU");
    client_socket
        .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
            offsets: vec![OffsetUpdate {
                offset: 20,
                session_id: stream_id,
            }],
        }))
        .await
        .expect("send offset");

    server_end_event.notify();
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_and_new_request() {
    let test_path = temp_dir().join("test_stream_fetch_filter_new_request");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::shared(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "testfilter";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    load_wasm_module("fluvio_wasm_filter", &ctx);

    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter".to_owned()),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let _stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let fetch_request = DefaultFetchRequest::default();
    let response = client_socket
        .send_and_receive(RequestMessage::new_request(fetch_request))
        .await;

    assert!(response.is_ok());

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_invalid_wasm_module() {
    let test_path = temp_dir().join("test_stream_fetch_invalid_wasm_module");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::shared(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "test_invalid_wasm";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    let wasm = Vec::from("Hello, world, I'm not a valid WASM module!");
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::AdHoc(wasm),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let response = stream
        .next()
        .await
        .expect("should get response")
        .expect("response should be Ok");

    assert!(
        matches!(
            response.partition.error_code,
            ErrorCode::SmartStreamError(SmartStreamError::InvalidWasmModule(_))
        ),
        "expected a SmartStream Module error for invalid WASM module"
    );

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_array_map() {
    let test_path = temp_dir().join("test_stream_fetch_array_map_json_array");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::shared(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "test_array_map";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // Input: One JSON record with 10 ints: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    let mut records = BatchProducer::builder()
        .records(1u16)
        .record_generator(Arc::new(|_, _| {
            let nums = (0..10).collect::<Vec<_>>();
            Record::new(serde_json::to_string(&nums).unwrap())
        }))
        .build()
        .expect("batch")
        .records();

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    load_wasm_module("fluvio_wasm_array_map_array", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_array_map_array".to_owned()),
        kind: SmartStreamKind::ArrayMap,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let response = stream
        .next()
        .await
        .expect("should get response")
        .expect("response should be Ok");

    assert_eq!(response.partition.records.batches.len(), 1);
    let batch = &response.partition.records.batches[0];

    // Output: 10 records containing integers 0-9
    for (i, record) in batch.records().iter().enumerate() {
        assert_eq!(
            record.value.as_ref(),
            RecordData::from(i.to_string()).as_ref()
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_filter_map() {
    let test_path = temp_dir().join("test_stream_fetch_filter_map");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::shared(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "test_filter_map";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // Input: the following records:
    //
    // 11
    // 22
    // 33
    // 44
    // 55
    let mut records = BatchProducer::builder()
        .records(5u16)
        .record_generator(Arc::new(|i, _| Record::new(((i + 1) * 11).to_string())))
        .build()
        .expect("batch")
        .records();

    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    load_wasm_module("fluvio_wasm_filter_map", &ctx);
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined("fluvio_wasm_filter_map".to_owned()),
        kind: SmartStreamKind::FilterMap,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let response = stream
        .next()
        .await
        .expect("should get response")
        .expect("response should be Ok");

    assert_eq!(response.partition.records.batches.len(), 1);
    let batch = &response.partition.records.batches[0];
    assert_eq!(batch.records().len(), 2);

    // Output:
    //
    // 11 -> _
    // 22 -> 11
    // 33 -> _
    // 44 -> 22
    // 55 -> _
    let records = batch.records();
    assert_eq!(records[0].value, RecordData::from(11.to_string()));
    assert_eq!(records[1].value, RecordData::from(22.to_string()));

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_filter_with_params() {
    use std::collections::BTreeMap;
    let test_path = temp_dir().join("test_stream_fetch_filter");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "testfilter_with_params";

    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    let mut params = BTreeMap::new();
    params.insert("key".to_string(), "b".to_string());

    load_wasm_module("fluvio_wasm_filter_with_parameters", &ctx);

    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(
            "fluvio_wasm_filter_with_parameters".to_owned(),
        ),
        kind: SmartStreamKind::Filter,
        params: params.into(),
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    // 1 out of 2 are filtered
    let mut records = create_filter_records(2);
    replica
        .write_record_set(&mut records, ctx.follower_notifier())
        .await
        .expect("write");

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    debug!("first filter fetch");
    let response = stream.next().await.expect("first").expect("response");
    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;

        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 1);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "b".repeat(100).as_bytes()
        );
        assert_eq!(batch.records()[0].get_offset_delta(), 0);

        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 2);
        assert_eq!(partition.next_offset_for_fetch(), Some(2));

        assert_eq!(partition.records.batches.len(), 1);
    }

    // Using default params
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(
            "fluvio_wasm_filter_with_parameters".to_owned(),
        ),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    debug!("second filter fetch");
    let response = stream.next().await.expect("first").expect("response");
    {
        debug!("received first message");
        assert_eq!(response.topic, topic);

        let partition = &response.partition;
        assert_eq!(partition.error_code, ErrorCode::None);
        assert_eq!(partition.high_watermark, 2);
        assert_eq!(partition.next_offset_for_fetch(), Some(2));

        assert_eq!(partition.records.batches.len(), 1);
        let batch = &partition.records.batches[0];
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.records().len(), 1);
        assert_eq!(
            batch.records()[0].value().as_ref(),
            "a".repeat(100).as_bytes()
        );
        assert_eq!(batch.records()[0].get_offset_delta(), 1);
    }

    server_end_event.notify();
}

#[fluvio_future::test(ignore)]
async fn test_stream_fetch_invalid_smartstream() {
    let test_path = temp_dir().join("test_stream_fetch_invalid_smartstream");
    ensure_clean_dir(&test_path);

    let addr = format!("127.0.0.1:{}", NEXT_PORT.fetch_add(1, Ordering::Relaxed));
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::shared(FluvioSocket::connect(&addr).await.expect("connect"));

    // perform for two versions
    let topic = "test_invalid_smartstream";
    let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    let wasm = include_bytes!("test_data/filter_missing_attribute.wasm").to_vec();
    let smart_module = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::AdHoc(wasm),
        kind: SmartStreamKind::Filter,
        ..Default::default()
    };

    let stream_request = DefaultStreamFetchRequest {
        topic: topic.to_owned(),
        partition: 0,
        fetch_offset: 0,
        isolation: Isolation::ReadUncommitted,
        max_bytes: 10000,
        wasm_module: Vec::new(),
        smart_module: Some(smart_module),
        ..Default::default()
    };

    let mut stream = client_socket
        .create_stream(RequestMessage::new_request(stream_request), 11)
        .await
        .expect("create stream");

    let response = stream
        .next()
        .await
        .expect("should get response")
        .expect("response should be Ok");

    match response.partition.error_code {
        ErrorCode::SmartStreamError(SmartStreamError::InvalidWasmModule(name)) => {
            assert_eq!(name, "invalid gzip header");
        }
        _ => panic!("expected an InvalidWasmModule error"),
    }

    server_end_event.notify();
    debug!("terminated controller");
}
