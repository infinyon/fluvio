use std::{env::temp_dir, time::Duration};

use dataplane::{
    produce::{DefaultProduceRequest, TopicProduceData, PartitionProduceData},
    api::RequestMessage,
};
use fluvio_controlplane_metadata::partition::Replica;
use fluvio_future::timer::sleep;
use fluvio_socket::{MultiplexerSocket, FluvioSocket};
use flv_util::fixture::ensure_clean_dir;
use tracing::debug;

use crate::{
    config::SpuConfig,
    core::GlobalContext,
    services::public::{create_public_server, tests::create_filter_records},
    replication::leader::LeaderReplicaState,
};

#[fluvio_future::test(ignore)]
async fn test_produce_basic() {
    let test_path = temp_dir().join("test_stream_fetch");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{}", port);
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
    let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
        .await
        .expect("replica");
    ctx.leaders_state().insert(test_id, replica.clone());

    // Make three produce requests with <records_per_request> records and check that returned offset is correct
    let records_per_request = 9;
    for i in 0..3 {
        let records = create_filter_records(records_per_request);

        let mut produce_request = DefaultProduceRequest {
            ..Default::default()
        };

        let partition_produce = PartitionProduceData {
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
            records: create_filter_records(records_per_request),
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
            produce_response.responses[0].partitions[idx as usize].base_offset as u16,
            records_per_request * (3 + idx)
        );
    }

    server_end_event.notify();
    debug!("terminated controller");
}
