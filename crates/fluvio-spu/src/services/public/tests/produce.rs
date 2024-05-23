use std::{env::temp_dir, time::Duration};

use fluvio::{SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind};
use fluvio_controlplane::replica::Replica;
use fluvio_smartmodule::{Record, dataplane::smartmodule::Lookback};
use fluvio_storage::{FileReplica, iterators::FileBatchIterator};
use tracing::debug;

use fluvio_protocol::{
    api::{RequestMessage, RequestKind},
    link::ErrorCode,
    Decoder,
};
use fluvio_controlplane_metadata::topic::{
    CompressionAlgorithm, Deduplication, Bounds, Filter, Transform,
};
use fluvio_future::timer::sleep;
use fluvio_socket::{MultiplexerSocket, FluvioSocket};
use fluvio_spu_schema::{
    produce::{
        DefaultProduceRequest, DefaultPartitionRequest, TopicProduceData, PartitionProduceData,
    },
    Isolation,
};
use flv_util::fixture::ensure_clean_dir;

use crate::{
    config::SpuConfig,
    core::GlobalContext,
    replication::leader::LeaderReplicaState,
    services::public::tests::{
        create_filter_raw_records, create_filter_records, create_public_server_with_root_auth,
        load_wasm_module, vec_to_raw_batch,
    },
};

#[fluvio_future::test(ignore)]
async fn test_produce_basic() {
    let test_path = temp_dir().join("produce_basic");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));
    let topic = "test_produce";
    let test = Replica::new((topic, 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");
    ctx.leaders_state().insert(test_id, replica.clone()).await;

    // Make three produce requests with <records_per_request> records and check that returned offset is correct
    let records_per_request = 9;
    for i in 0..3 {
        let records = create_filter_records(records_per_request)
            .try_into()
            .expect("filter records");

        let mut produce_request = DefaultProduceRequest {
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            produce_response.responses[0].partitions[0].base_offset as u16,
            records_per_request * i
        );
    }

    // Make a produce request with multiple Partition produce requests and check that each returned offset is correct
    let mut produce_request = DefaultProduceRequest {
        ..Default::default()
    };

    let partitions = (0..2)
        .map(|_| PartitionProduceData {
            partition_index: 0,
            records: create_filter_records(records_per_request)
                .try_into()
                .expect("partition"),
        })
        .collect::<Vec<_>>();
    let topic_produce_request = TopicProduceData {
        name: topic.to_owned(),
        partitions,
        ..Default::default()
    };

    produce_request.topics.push(topic_produce_request);

    let produce_response = client_socket
        .send_and_receive(RequestMessage::new_request(produce_request))
        .await
        .expect("send offset");

    // Check base offset
    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(produce_response.responses[0].partitions.len(), 2);

    for idx in 0_u16..2_u16 {
        assert_eq!(
            produce_response.responses[0].partitions[idx as usize].error_code,
            ErrorCode::None
        );
        assert_eq!(
            produce_response.responses[0].partitions[idx as usize].base_offset as u16,
            records_per_request * (3 + idx)
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_produce_invalid_compression() {
    let test_path = temp_dir().join("produce_invalid_compression");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));
    let topic = "test_produce";
    let mut test = Replica::new((topic, 0), 5001, vec![5001]);
    test.compression_type = CompressionAlgorithm::Gzip;
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");

    ctx.leaders_state().insert(test_id, replica.clone()).await;

    // Make three produce requests with <records_per_request> records and check that returned offset is correct
    let records_per_request = 9;
    let records = create_filter_records(records_per_request)
        .try_into()
        .expect("filter records");

    let mut produce_request = DefaultProduceRequest {
        ..Default::default()
    };

    let partition_produce = DefaultPartitionRequest {
        partition_index: 0,
        records,
    };
    let topic_produce_request = TopicProduceData {
        name: topic.to_owned(),
        partitions: vec![partition_produce],
        ..Default::default()
    };

    produce_request.topics.push(topic_produce_request);

    let produce_response = client_socket
        .send_and_receive(RequestMessage::new_request(produce_request))
        .await
        .expect("send offset");

    // Check error code
    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(produce_response.responses[0].partitions.len(), 1);
    assert_eq!(
        produce_response.responses[0].partitions[0].error_code,
        ErrorCode::CompressionError
    );

    server_end_event.notify();
    debug!("terminated controller");
}
use crate::replication::test::TestConfig;
use crate::services::create_internal_server;

#[fluvio_future::test(ignore)]
async fn test_produce_request_timed_out() {
    let config = TestConfig::builder()
        .followers(1_u16)
        .base_port(14010_u16)
        .generate("produce_request_timed_out");

    let public_addr = config.leader_public_addr();

    let (leader_ctx, _) = config.leader_replica().await;

    let server_end_event =
        create_public_server_with_root_auth(public_addr.to_owned(), leader_ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&public_addr).await.expect("connect"));
    let topic = "test";

    let records_per_request = 5;
    let records = create_filter_records(records_per_request)
        .try_into()
        .expect("filter records");

    let mut produce_request = DefaultProduceRequest {
        isolation: Isolation::ReadCommitted,
        timeout: Duration::from_millis(300),
        ..Default::default()
    };

    let partition_produce = DefaultPartitionRequest {
        partition_index: 0,
        records,
    };
    let topic_produce_request = TopicProduceData {
        name: topic.to_owned(),
        partitions: vec![partition_produce],
        ..Default::default()
    };

    produce_request.topics.push(topic_produce_request);

    let produce_response = client_socket
        .send_and_receive(RequestMessage::new_request(produce_request))
        .await
        .expect("send offset");

    // Check error code
    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(produce_response.responses[0].partitions.len(), 1);
    assert_eq!(
        produce_response.responses[0].partitions[0].error_code,
        ErrorCode::RequestTimedOut {
            timeout_ms: 300,
            kind: RequestKind::Produce
        }
    );

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_produce_not_waiting_replication() {
    let config = TestConfig::builder()
        .followers(1_u16)
        .base_port(14020_u16)
        .generate("produce_request_timed_out");

    let (leader_ctx, _) = config.leader_replica().await;
    let public_addr = config.leader_public_addr();

    let server_end_event =
        create_public_server_with_root_auth(public_addr.to_owned(), leader_ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&public_addr).await.expect("connect"));
    let topic = "test";

    let records_per_request = 5;
    let records = create_filter_records(records_per_request)
        .try_into()
        .expect("filter records");

    let mut produce_request = DefaultProduceRequest {
        isolation: Isolation::ReadUncommitted,
        timeout: Duration::from_millis(300),
        ..Default::default()
    };

    let partition_produce = DefaultPartitionRequest {
        partition_index: 0,
        records,
    };
    let topic_produce_request = TopicProduceData {
        name: topic.to_owned(),
        partitions: vec![partition_produce],
        ..Default::default()
    };

    produce_request.topics.push(topic_produce_request);

    let produce_response = client_socket
        .send_and_receive(RequestMessage::new_request(produce_request))
        .await
        .expect("send offset");

    // Check error code
    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(produce_response.responses[0].partitions.len(), 1);
    assert_eq!(
        produce_response.responses[0].partitions[0].error_code,
        ErrorCode::None
    );

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_produce_waiting_replication() {
    let config = TestConfig::builder()
        .followers(1_u16)
        .base_port(14030_u16)
        .generate("produce_waiting_replication");

    let (leader_ctx, leader_replica) = config.leader_replica().await;
    let public_addr = config.leader_public_addr();

    let public_server_end_event =
        create_public_server_with_root_auth(public_addr.to_owned(), leader_ctx.clone()).run();

    let private_server_end_event =
        create_internal_server(config.leader_addr(), leader_ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    debug!("starting follower replica controller");
    config.follower_replica(0).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&public_addr).await.expect("connect"));
    let topic = "test";

    let records_per_request = 5;
    let records = create_filter_records(records_per_request)
        .try_into()
        .expect("filter records");

    let mut produce_request = DefaultProduceRequest {
        isolation: Isolation::ReadCommitted,
        timeout: Duration::from_millis(10000),
        ..Default::default()
    };

    let partition_produce = DefaultPartitionRequest {
        partition_index: 0,
        records,
    };
    let topic_produce_request = TopicProduceData {
        name: topic.to_owned(),
        partitions: vec![partition_produce],
        ..Default::default()
    };

    produce_request.topics.push(topic_produce_request);

    let produce_response = client_socket
        .send_and_receive(RequestMessage::new_request(produce_request))
        .await
        .expect("send offset");

    // Check error code
    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(produce_response.responses[0].partitions.len(), 1);
    assert_eq!(
        produce_response.responses[0].partitions[0].error_code,
        ErrorCode::None
    );
    assert_eq!(produce_response.responses[0].partitions[0].base_offset, 0);
    assert_eq!(leader_replica.hw(), 5);
    assert_eq!(leader_replica.leo(), 5);

    public_server_end_event.notify();
    private_server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_produce_metrics() {
    let test_path = temp_dir().join("produce_basic_metrics");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));
    let topic = "test_produce";
    let test = Replica::new((topic, 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");

    ctx.leaders_state().insert(test_id, replica.clone()).await;

    assert_eq!(ctx.metrics().inbound().client_bytes(), 0);
    assert_eq!(ctx.metrics().inbound().client_records(), 0);
    assert_eq!(ctx.metrics().inbound().connector_bytes(), 0);
    assert_eq!(ctx.metrics().inbound().connector_records(), 0);

    {
        let records = create_filter_records(10)
            .try_into()
            .expect("filter records");

        let mut produce_request = DefaultProduceRequest {
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let _ = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        assert_eq!(ctx.metrics().inbound().client_bytes(), 1151);
        assert_eq!(ctx.metrics().inbound().client_records(), 10);
        assert_eq!(ctx.metrics().inbound().connector_bytes(), 0);
        assert_eq!(ctx.metrics().inbound().connector_records(), 0);
    }
    {
        let records = create_filter_records(5).try_into().expect("filter records");
        let mut produce_request = DefaultProduceRequest {
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let _ = client_socket
            .send_and_receive(
                RequestMessage::new_request(produce_request).set_client_id("fluvio_connector"),
            )
            .await
            .expect("send offset");

        assert_eq!(ctx.metrics().inbound().client_bytes(), 1151);
        assert_eq!(ctx.metrics().inbound().client_records(), 10);
        assert_eq!(ctx.metrics().inbound().connector_bytes(), 606);
        assert_eq!(ctx.metrics().inbound().connector_records(), 5);
    }
    server_end_event.notify();
    debug!("terminated controller");
}

const FLUVIO_WASM_FILTER_WITH_LOOKBACK: &str = "fluvio_smartmodule_filter_lookback";

#[fluvio_future::test(ignore)]
async fn test_produce_basic_with_smartmodule_with_lookback() {
    let test_path = temp_dir().join("test_produce_basic_with_smartmodule_with_lookback");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    load_wasm_module(&ctx, FLUVIO_WASM_FILTER_WITH_LOOKBACK);
    let mut smartmodule = SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(FLUVIO_WASM_FILTER_WITH_LOOKBACK.to_owned()),
        kind: SmartModuleKind::Filter,
        params: Default::default(),
    };
    smartmodule.params.set_lookback(Some(Lookback::last(1)));
    let mut smartmodules = vec![smartmodule];

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));
    let topic = "test_produce_basic_with_smartmodule_with_lookback";
    let test = Replica::new((topic, 0), 5001, vec![5001]);
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");

    ctx.leaders_state().insert(test_id, replica.clone()).await;

    {
        // no records before, smartmodule allows all
        let records = vec_to_raw_batch(&["1", "2", "3"]);

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(read_records(&replica).await, vec!["1", "2", "3"]);
    }
    {
        // the last record is 3, smartmodule allows only ones that greater than 3
        let records = vec_to_raw_batch(&["1", "2", "3", "4", "5"]);

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(read_records(&replica).await, vec!["1", "2", "3", "4", "5"]);
    }
    {
        // if last = 0, no records should be read on look_back, sm allows all
        for sm in smartmodules.iter_mut() {
            sm.params.set_lookback(Some(Lookback::last(0)));
        }

        let records = vec_to_raw_batch(&["1", "2"]);

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "1", "2"]
        );
    }

    {
        // if lookback parameter is not present, sm allows all
        for sm in smartmodules.iter_mut() {
            sm.params = Default::default();
        }

        let records = vec_to_raw_batch(&["1", "2"]);

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "1", "2", "1", "2"]
        );
    }

    {
        // age lookback returns all records, so, only the one that greater than 2 will be recorded
        for sm in smartmodules.iter_mut() {
            sm.params
                .set_lookback(Some(Lookback::age(Duration::from_secs(60 * 60), None)));
        }

        let records = vec_to_raw_batch(&["3", "4"]);

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "1", "2", "1", "2", "3", "4"]
        );
    }

    {
        // age lookback returns none records, so, all records allowed
        for sm in smartmodules.iter_mut() {
            sm.params
                .set_lookback(Some(Lookback::age(Duration::from_millis(1), None)));
        }

        let records = vec_to_raw_batch(&["1", "2"]);
        std::thread::sleep(Duration::from_millis(50));

        let mut produce_request = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            ..Default::default()
        };

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "1", "2", "1", "2", "3", "4", "1", "2"]
        );
    }

    {
        // error from look_back call is propagated
        for sm in smartmodules.iter_mut() {
            sm.params.set_lookback(Some(Lookback::last(2)));
        }

        // insert wrong last record without sm
        let produce_request1 = DefaultProduceRequest {
            topics: vec![TopicProduceData {
                name: topic.to_owned(),
                partitions: vec![DefaultPartitionRequest {
                    partition_index: 0,
                    records: vec_to_raw_batch(&["wrong last record"]),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let _ = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request1))
            .await
            .expect("send offset");

        let produce_request2 = DefaultProduceRequest {
            smartmodules: smartmodules.clone(),
            topics: vec![TopicProduceData {
                name: topic.to_owned(),
                partitions: vec![DefaultPartitionRequest {
                    partition_index: 0,
                    records: vec_to_raw_batch(&["4"]),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request2))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            produce_response.responses[0].partitions[0].error_code,
            ErrorCode::SmartModuleLookBackError("invalid digit found in string\n\nSmartModule Lookback Error: \n    Offset: 0\n    Key: NULL\n    Value: wrong last record".to_string())
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}

const FLUVIO_WASM_DEDUPLICATION_FILTER: &str = "fluvio_smartmodule_filter_hashset";

#[fluvio_future::test(ignore)]
async fn test_produce_with_deduplication() {
    let test_path = temp_dir().join("test_produce_with_deduplication");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    load_wasm_module(&ctx, FLUVIO_WASM_DEDUPLICATION_FILTER);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let deduplication = Deduplication {
        bounds: Bounds {
            count: 6,
            age: None,
        },
        filter: Filter {
            transform: Transform {
                uses: FLUVIO_WASM_DEDUPLICATION_FILTER.to_owned(),
                with: Default::default(),
            },
        },
    };
    let topic = "test_produce_with_deduplication";
    let mut test = Replica::new((topic, 0), 5001, vec![5001]);
    test.deduplication = Some(deduplication);
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test.clone(), ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");

    ctx.leaders_state()
        .insert(test_id.clone(), replica.clone())
        .await;

    {
        // dedup declines repeated record within one batch
        let records = vec_to_raw_batch(&["1", "2", "3", "1"]);

        let mut produce_request: DefaultProduceRequest = Default::default();

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(read_records(&replica).await, vec!["1", "2", "3"]);
    }

    {
        // dedup declines repeated record from other batches as well
        let records = vec_to_raw_batch(&["1", "2", "4", "5", "6"]);

        let mut produce_request: DefaultProduceRequest = Default::default();

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "6"]
        );
    }

    {
        // if window closed, duplicate allowed
        let records = vec_to_raw_batch(&["7", "1", "2"]);

        let mut produce_request: DefaultProduceRequest = Default::default();

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "6", "7", "1", "2"]
        );
    }

    {
        drop(replica);
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica")
            .init(&ctx)
            .await
            .expect("init succeeded");

        ctx.leaders_state().insert(test_id, replica.clone()).await;

        // if replica re-created, lookback should read the previous state
        let records = vec_to_raw_batch(&["7", "8"]);

        let mut produce_request: DefaultProduceRequest = Default::default();

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert_eq!(
            read_records(&replica).await,
            vec!["1", "2", "3", "4", "5", "6", "7", "1", "2", "8"]
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_produce_smart_engine_memory_overfow() {
    let test_path = temp_dir().join("test_produce_smart_engine_memory_overfow");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    let max_memory_size = 1179648;
    spu_config.smart_engine.store_max_memory = max_memory_size;
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    load_wasm_module(&ctx, FLUVIO_WASM_DEDUPLICATION_FILTER);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let deduplication = Deduplication {
        bounds: Bounds {
            count: 6,
            age: None,
        },
        filter: Filter {
            transform: Transform {
                uses: FLUVIO_WASM_DEDUPLICATION_FILTER.to_owned(),
                with: Default::default(),
            },
        },
    };
    let topic = "test_produce_with_deduplication";
    let mut test = Replica::new((topic, 0), 5001, vec![5001]);
    test.deduplication = Some(deduplication);
    let test_id = test.id.clone();
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let replica = LeaderReplicaState::create(test.clone(), ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica")
        .init(&ctx)
        .await
        .expect("init succeeded");

    ctx.leaders_state()
        .insert(test_id.clone(), replica.clone())
        .await;

    {
        // dedup declines repeated record within one batch
        let records = create_filter_raw_records(1000);

        let mut produce_request: DefaultProduceRequest = Default::default();

        let partition_produce = DefaultPartitionRequest {
            partition_index: 0,
            records,
        };
        let topic_produce_request = TopicProduceData {
            name: topic.to_owned(),
            partitions: vec![partition_produce],
            ..Default::default()
        };

        produce_request.topics.push(topic_produce_request);

        let produce_response = client_socket
            .send_and_receive(RequestMessage::new_request(produce_request))
            .await
            .expect("send offset");

        // Check base offset
        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(produce_response.responses[0].partitions.len(), 1);
        assert!(
            matches!(produce_response.responses[0].partitions[0].error_code, ErrorCode::SmartModuleMemoryLimitExceeded { requested: _, max } if max == max_memory_size as u64)
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}

#[fluvio_future::test(ignore)]
async fn test_dedup_init_smart_engine_memory_overfow() {
    let test_path = temp_dir().join("test_dedup_init_smart_engine_memory_overfow");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    let max_memory_size = 1024;
    spu_config.smart_engine.store_max_memory = max_memory_size;
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);
    load_wasm_module(&ctx, FLUVIO_WASM_DEDUPLICATION_FILTER);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let deduplication = Deduplication {
        bounds: Bounds {
            count: 6,
            age: None,
        },
        filter: Filter {
            transform: Transform {
                uses: FLUVIO_WASM_DEDUPLICATION_FILTER.to_owned(),
                with: Default::default(),
            },
        },
    };
    let topic = "test_produce_with_deduplication";
    let mut test = Replica::new((topic, 0), 5001, vec![5001]);
    test.deduplication = Some(deduplication);
    ctx.replica_localstore().sync_all(vec![test.clone()]);

    let init_res: Result<LeaderReplicaState<FileReplica>, anyhow::Error> =
        LeaderReplicaState::create(test.clone(), ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica")
            .init(&ctx)
            .await;

    assert!(init_res.is_err());
    assert!(
        matches!(init_res.unwrap_err().downcast::<ErrorCode>(), Ok(ErrorCode::SmartModuleMemoryLimitExceeded { requested: _, max }) if max == max_memory_size as u64)
    );

    server_end_event.notify();
    debug!("terminated controller");
}

async fn read_records(replica: &LeaderReplicaState<FileReplica>) -> Vec<String> {
    let slice = replica
        .read_records(0i64, u32::MAX, Isolation::ReadUncommitted)
        .await
        .expect("read records");
    if let Some(file_slice) = slice.file_slice {
        let file_batch_iterator = FileBatchIterator::from_raw_slice(file_slice);

        let mut result = Vec::new();
        for batch_result in file_batch_iterator {
            let input_batch = batch_result.expect("batch");

            let mut records: Vec<Record> = vec![];
            Decoder::decode(
                &mut records,
                &mut std::io::Cursor::new(input_batch.records),
                0,
            )
            .expect("decoded");
            result.append(&mut records);
        }
        result
            .into_iter()
            .map(|r| r.into_value().as_utf8_lossy_string().to_string())
            .collect()
    } else {
        Vec::new()
    }
}
