use std::{env::temp_dir, time::Duration};

use tracing::debug;

use fluvio_protocol::{
    api::{RequestMessage, RequestKind},
    link::ErrorCode,
};
use fluvio_controlplane_metadata::{partition::Replica, topic::CompressionAlgorithm};
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
    services::public::{create_public_server, tests::create_filter_records},
    replication::leader::LeaderReplicaState,
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

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

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
        .expect("replica");
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

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

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
        .expect("replica");
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

    let server_end_event = create_public_server(public_addr.clone(), leader_ctx.clone()).run();

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

    let server_end_event = create_public_server(public_addr.clone(), leader_ctx.clone()).run();

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
        create_public_server(public_addr.clone(), leader_ctx.clone()).run();
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

    let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

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
        .expect("replica");
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
